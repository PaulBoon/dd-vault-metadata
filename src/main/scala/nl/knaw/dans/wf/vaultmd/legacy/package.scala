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
package nl.knaw.dans.wf.vaultmd

import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, ControlledSingleValueField, MetadataBlock, MetadataField, PrimitiveMultiValueField, PrimitiveSingleValueField }
import org.json4s.{ CustomSerializer, DefaultFormats, Extraction, Formats, JNull, JObject }

import java.util
import scala.collection.mutable
import collection.JavaConverters._

package object legacy {
  implicit val jsonFormats: Formats = DefaultFormats + MetadataFieldSerializer + MetadataBlockSerializer



  case class LockRecord(lockType: String, date: String, user: String)
  case class LockStatusMessage(status: String, data: List[LockRecord])
  case class RequestFailedException(status: Int, msg: String, body: String) extends Exception(s"Command could not be executed. Server returned: status line: '$msg', body: '$body'")
  case class ExternalSystemCallException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
  case class InvocationIdNotFoundException(numberOfTimesTried: Int, waitTimeInMilliseconds: Int) extends RuntimeException(s"Workflow was not paused. Number of tries = $numberOfTimesTried, wait time between tries = $waitTimeInMilliseconds ms.")

  type JsonObject = Map[String, MetadataField]

  case class MetadataBlockScala(displayName: String, name: String, fields: List[MetadataField]) extends MetadataBlock {
    override def getFields: util.List[MetadataField] = { fields.asJava }
  }

  case class FieldMap() {
    private val fields = mutable.Map[String, MetadataField]()

    def addPrimitiveField(name: String, value: String): Unit = {
      fields.put(name, new PrimitiveSingleValueField(name, value))
    }

    def addCvField(name: String, value: String): Unit = {
      fields.put(name, new ControlledSingleValueField(name, value))
    }

    def toJsonObject: JsonObject = fields.toMap
  }

  object MetadataFieldSerializer extends CustomSerializer[MetadataField](format => ( {
    case jsonObj: JObject =>
      val multiple = (jsonObj \ "multiple").extract[Boolean]
      val typeClass = (jsonObj \ "typeClass").extract[String]

      typeClass match {
        case "primitive" if !multiple => Extraction.extract[PrimitiveSingleValueField](jsonObj)
        case "primitive" => Extraction.extract[PrimitiveMultiValueField](jsonObj)
        case "controlledVocabulary" if !multiple => Extraction.extract[PrimitiveSingleValueField](jsonObj)
        case "controlledVocabulary" => Extraction.extract[PrimitiveMultiValueField](jsonObj)
        case "compound" => Extraction.extract[CompoundField](jsonObj)
      }
  }, {
    case null => JNull
  }
  ))

  object MetadataBlockSerializer extends CustomSerializer[MetadataBlock](format => ( {
    case jsonObj: JObject =>
      Extraction.extract[MetadataBlockScala](jsonObj)
  }, {
    case null => JNull
  }
  ))
}


