/*
 * DATABRICKS CONFIDENTIAL & PROPRIETARY
 * __________________
 *
 * Copyright 2024-present Databricks, Inc.
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
 * and its suppliers, if any.  The intellectual and technical concepts contained herein are
 * proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
 * patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
 * or reproduction of this information is strictly forbidden unless prior written permission is
 * obtained from Databricks, Inc.
 *
 * If you view or obtain a copy of this information and believe Databricks, Inc. may not have
 * intended it to be made available, please promptly report it to Databricks Legal Department
 * @ legal@databricks.com.
 */

package com.databricks.sql.catalyst.unstructured

import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import scala.sys.process._
import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.databricks.logging.proto.UnstructuredEvent.{Backend, UnstructuredEventType}
import shaded.databricks.tess4j.net.sourceforge.tess4j._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

/**
 * An implementation of [[UnstructuredBackend]] that uses Tesseract OCR for metadata extraction.
 */
class TesseractUnstructuredBackendImpl(protected val options: UnstructuredOptions)
  extends UnstructuredToMetadataBackend {

  override protected val backendType: Backend = Backend.TESSERACT

  /**
   * Create a new Tesseract API handle to interact with.
   */
  private lazy val handle: Tesseract = {
    val handle = new Tesseract()
    handle.setLanguage("eng")
    handle.setDatapath(TesseractBackendUtils.TESS_DATA_DIR)
    // https://github.com/nguyenq/tess4j/issues/157#issuecomment-514363084
    handle.setTessVariable("user_defined_dpi", options.dpi)
    handle
  }

  /**
   * Converts an image byte array to text using Tesseract OCR.
   * @param imageBytes The byte array of the image.
   * @return The text extracted from the image.
   */
  private def imageBytesToText(imageBytes: Array[Byte]): String = {
    val byteStream = new ByteArrayInputStream(imageBytes)
    val bufferedImage = ImageIO.read(byteStream)
    handle.doOCR(bufferedImage)
  }

  protected def getOcrAndPageLengthInBytes(data: Array[Byte]): (Seq[String], Seq[Long]) = {
    val imageBlob = convertToImageBlob(data)
    (imageBlob.map(imageBytesToText), imageBlob.map(_.length.toLong))
  }

  /**
   * Extract OCR text from the given bytes using Tesseract OCR.
   * If the data is a multi-page PDF, we will concatenate the OCR text results.
   */
  override def extractOcrText(data: Array[Byte], timezoneId: String): String = {
    runWithLogging { state =>
      state.eventType = Some(UnstructuredEventType.EXTRACT_OCR)
      val (texts, pageLengthInBytes) = getOcrAndPageLengthInBytes(data)
      state.pageLengthInBytes = pageLengthInBytes
      texts.mkString("\n")
    }
  }

  /**
   * Extract possible metadata from the given bytes using Tesseract OCR.
   * If the data is a multi-page PDF, we will concatenate the OCR text results.
   */
  override def extractLayoutMetadata(
      data: Array[Byte],
      timezoneId: String): UnstructuredMetadata = {
    runWithLogging { state =>
      state.eventType = Some(UnstructuredEventType.EXTRACT_METADATA)
      val (texts, pageLengthInBytes) = getOcrAndPageLengthInBytes(data)
      state.pageLengthInBytes = pageLengthInBytes
      TesseractUnstructuredMetadata(texts)
    }
  }
}

/**
 * Unstructured metadata extracted by Tesseract OCR.
 */
case class TesseractUnstructuredMetadata(texts: Seq[String]) extends UnstructuredMetadata {
  override def getData: InternalRow = {
    val pages = texts.zipWithIndex.map { case (text, pageIndex) =>
      InternalRow.apply(
        pageIndex,                 // page_index
        null,                      // detected_page_number
        InternalRow.apply(         // representation
          UTF8String.fromString(text), // text
          null                         // markdown
        ),
        null,                      // header
        null                       // footer
      )
    }
    InternalRow.apply(
      InternalRow.apply(null, null),     // document
      ArrayData.toArrayData(pages),      // pages
      ArrayData.toArrayData(Array.empty) // elements
    )
  }
}

object TesseractBackendUtils {

  // Look up the local training data directory by invoking the Tesseract binary
  lazy val TESS_DATA_DIR: String = {
    try {
      val availableLanguagesOutput = "tesseract --list-langs".!!
      val pathRegex: Regex = """(/(?:[a-zA-Z0-9._-]+/)*[a-zA-Z0-9._-]+)""".r
      val tessDataDir = pathRegex.findAllIn(availableLanguagesOutput).toSeq.headOption.getOrElse {
        throw new IllegalStateException(
          s"Unable to find Tesseract data directory from command output " +
            availableLanguagesOutput
        )
      }
      if (!availableLanguagesOutput.contains("eng")) {
        throw new IllegalStateException(
          s"English language model not found in Tesseract data directory $tessDataDir"
        )
      }
      tessDataDir
    } catch {
      case NonFatal(e) =>
        throw UnstructuredErrors.failedToUseTesseractOCR(e)
    }
  }
}

