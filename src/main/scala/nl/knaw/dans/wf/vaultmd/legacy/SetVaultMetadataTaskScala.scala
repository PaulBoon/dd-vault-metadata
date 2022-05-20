/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.wf.vaultmd.legacy

import nl.knaw.dans.lib.dataverse.model.dataset._
import nl.knaw.dans.lib.dataverse.model.workflow.ResumeMessage
import nl.knaw.dans.lib.dataverse.{ DataverseClient, DataverseException, DataverseHttpResponse }
import nl.knaw.dans.lib.error.TryExtensions
import nl.knaw.dans.wf.vaultmd.core.SetVaultMetadataTask
import org.json4s.native.JsonMethods
import org.json4s.{ JObject, JValue }
import org.slf4j.LoggerFactory

import java.lang.Thread._
import java.net.HttpURLConnection._
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class SetVaultMetadataTaskScala(workFlowVariables: WorkflowVariables, dataverse: DataverseClient, nbnPrefix: String, maxNumberOfRetries: Int, timeBetweenRetries: Int) {
  private val logger = LoggerFactory.getLogger(classOf[SetVaultMetadataTask])

  require(nbnPrefix != null)
  private val dataset = dataverse.dataset(workFlowVariables.globalId, workFlowVariables.invocationId)

  def run(): Try[Unit] = {
    (for {
      _ <- Try { dataset.awaitLock("Workflow") }
      _ <- editVaultMetadata()
      _ <- resumeWorkflow(workFlowVariables.invocationId)
      _ = logger.info(s"Vault metadata set for dataset ${ workFlowVariables.globalId }. Dataset resume called.")
    } yield ())
      .recover {
        case NonFatal(e) =>
          logger.error(s"SetVaultMetadataTask for dataset ${ workFlowVariables.globalId } failed. Resuming dataset with 'fail=true'", e)
          dataverse.workflows().resume(workFlowVariables.invocationId, new ResumeMessage("Failure", e.getMessage, "Publication failed: pre-publication workflow returned an error"))
      }
  }

  private def editVaultMetadata(): Try[Unit] = {
    logger.trace("ENTER")
    for {
      draftDsvJson <- getDatasetVersion(":draft")
      optLatestPublishedOrDeaccessionedDsvJson <- if (hasPreviousVersions(workFlowVariables)) getLatestsPublishedOrDeaccessionedVersion.map(Option(_))
                                                  else Success(None)
      bagId = getBagId(getVaultMetadataFieldValue(draftDsvJson, "dansBagId"), optLatestPublishedOrDeaccessionedDsvJson, workFlowVariables)
      nbn = optLatestPublishedOrDeaccessionedDsvJson
        .map(pdsv => getVaultMetadataFieldValue(pdsv, "dansNbn")
          .getOrElse(throw new IllegalStateException("Found published dataset-version without NBN")))
        .getOrElse(getVaultMetadataFieldValue(draftDsvJson, "dansNbn")
          .getOrElse(mintUrnNbn()))
      vaultFieldsToUpdate = createFieldList(workFlowVariables, bagId, nbn)
      _ <- Try { dataset.editMetadata(vaultFieldsToUpdate, true) }
      _ = logger.debug("editMetadata call returned success. Data Vault Metadata should be added to Dataverse now.")
    } yield ()
  }

  private def getDatasetVersion(version: String): Try[JValue] = {
    for {
      response <- Try { dataset.getVersion(version) }
      dsv <- Try { response.getEnvelopeAsString }
      json <- Try { JsonMethods.parse(dsv) }
    } yield json
  }

  private def getLatestsPublishedOrDeaccessionedVersion: Try[JValue] = {
    for {
      response <- Try { dataset.getAllVersions }
      dsv <- Try { response.getEnvelopeAsString }
      json <- Try { JsonMethods.parse(dsv) }
      versions = (json \ "data").extract[List[JObject]]
      nonDraftVersions = versions.filter(v => (List("RELEASED", "DEACCESSIONED").contains((v \ "versionState").extract[String])))
      latest = nonDraftVersions.maxBy(v => ((v \ "versionNumber").extract[String], (v \ "versionMinorNumber").extract[String]))
    } yield latest
  }

  private def getBagId(optFoundBagId: Option[String], optLatestPublishedDatasetVersion: Option[JValue], w: WorkflowVariables): String = {
    logger.trace(s"$optFoundBagId $w")
    optLatestPublishedDatasetVersion.map {
      latestPublishedDsv => { // Draft of version > 1.0
        val latestPublishedBagId = getVaultMetadataFieldValue(latestPublishedDsv, "dansBagId")
          .getOrElse(throw new IllegalArgumentException("Dataset with a latest published version without bag ID found!"))
        if (optFoundBagId.isEmpty || latestPublishedBagId == optFoundBagId.get) {
          /*
           * This happens after publishing a new version via the UI. The bagId from the previous version is inherited by the new draft. However, we
           * want every version to have a unique bagId.
           */
          mintBagId()
        }
        else {
          /*
           * Provided by machine deposit.
           */
          optFoundBagId.get
        }
      }
    }.getOrElse { // Draft of version 1.0
      optFoundBagId.getOrElse(mintBagId())
    }
  }

  private def getVaultMetadataFieldValue(dsvJson: JValue, fieldId: String): Option[String] = {
    val pvmd = dsvJson \\ "dansDataVaultMetadata"
    if (pvmd.children.isEmpty) Option.empty
    else {
      Try(pvmd.extract[MetadataBlock]).toOption
        .flatMap(_.getFields.asScala
          .map(_.asInstanceOf[PrimitiveSingleValueField])
          .find(_.getTypeName == fieldId))
        .map(_.getValue)
    }
  }

  private def hasPreviousVersions(w: WorkflowVariables): Boolean = {
    s"${ w.majorVersion }.${ w.minorVersion }" != "1.0"
  }

  private def createFieldList(workFlowVariables: WorkflowVariables,
                              bagId: String,
                              nbn: String,
                             ): FieldList = {
    logger.trace(s"$workFlowVariables $bagId $nbn")
    val fields = ListBuffer[MetadataField]()
    fields.append(new PrimitiveSingleValueField("dansDataversePid", workFlowVariables.globalId))
    fields.append(new PrimitiveSingleValueField("dansDataversePidVersion", s"${ workFlowVariables.majorVersion }.${ workFlowVariables.minorVersion }"))
    fields.append(new PrimitiveSingleValueField("dansBagId", bagId))
    fields.append(new PrimitiveSingleValueField("dansNbn", nbn))
    val fieldlist = new FieldList()
    fieldlist.setFields(fields.toList.asJava)
    fieldlist
  }

  private def mintUrnNbn(): String = {
    logger.trace("ENTER")
    "urn:nbn:" + nbnPrefix + UUID.randomUUID().toString
  }

  private def mintBagId(): String = {
    logger.trace("ENTER")
    "urn:uuid:" + UUID.randomUUID().toString
  }

  private def resumeWorkflow(invocationId: String): Try[Unit] = {
    logger.trace(s"$maxNumberOfRetries $timeBetweenRetries")
    var numberOfTimesTried = 0
    var invocationIdNotFound = true

    do {
      val resumeResponse = Try { dataverse.workflows().resume(invocationId, new ResumeMessage("Success", "", "")) }
      invocationIdNotFound = checkForInvocationIdNotFoundError(resumeResponse, invocationId).unsafeGetOrThrow

      if (invocationIdNotFound) {
        logger.debug(s"Sleeping $timeBetweenRetries ms before next try..")
        sleep(timeBetweenRetries)
        numberOfTimesTried += 1
      }
    } while (numberOfTimesTried <= maxNumberOfRetries && invocationIdNotFound)

    if (invocationIdNotFound) {
      logger.error(s"Workflow could not be resumed for dataset ${ workFlowVariables.globalId }. Number of retries: $maxNumberOfRetries. Time between retries: $timeBetweenRetries")
      Failure(InvocationIdNotFoundException(maxNumberOfRetries, timeBetweenRetries))
    }
    else Success(())
  }

  private def checkForInvocationIdNotFoundError(resumeResponse: Try[DataverseHttpResponse[AnyRef]], invocationId: String): Try[Boolean] = {
    resumeResponse.map(r => isError(r.getHttpResponse.getStatusLine.getStatusCode))
      .recover { case e: DataverseException if e.getStatus == HTTP_NOT_FOUND => true }
      .recoverWith { case e: Throwable => Failure(ExternalSystemCallException(s"Resume could not be called for dataset: $invocationId ", e)) }
  }

  private def isError(status: Int) = {
    status >= 400
  }
}
