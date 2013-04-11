package org.apache.blur.analysis;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.NumericUtils;
import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;

public final class IntegerAnalyzer extends Analyzer {

  public static int PRECISION_STEP_DEFAULT = NumericUtils.PRECISION_STEP_DEFAULT;
  public static int RADIX_DEFAULT = 10;
  private int radix = 10;
  private int precisionStep;

  public IntegerAnalyzer() {
    this(PRECISION_STEP_DEFAULT, RADIX_DEFAULT);
  }

  public IntegerAnalyzer(int precisionStep) {
    this(precisionStep, RADIX_DEFAULT);
  }

  public IntegerAnalyzer(int precisionStep, int radix) {
    this.precisionStep = precisionStep;
    this.radix = radix;
  }

  public int getRadix() {
    return radix;
  }

  public void setRadix(int radix) {
    this.radix = radix;
  }

  public int getPrecisionStep() {
    return precisionStep;
  }

  public void setPrecisionStep(int precisionStep) {
    this.precisionStep = precisionStep;
  }

  private int toInteger(Reader reader) throws IOException {
    StringBuilder builder = new StringBuilder(20);
    int read;
    while ((read = reader.read()) != -1) {
      builder.append((char) read);
    }
    return Integer.parseInt(builder.toString(), radix);
  }
  
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final CharTokenizer source = new CharTokenizer(LUCENE_VERSION, reader) {
      @Override
      protected boolean isTokenChar(int arg0) {
        return true;
      }
    };

    final int value;
    try {
      value = toInteger(reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final NumericTokenStream numericTokenStream = new NumericTokenStream(
        precisionStep);
    numericTokenStream.setIntValue(value);

    return new TokenStreamComponents(source, numericTokenStream) {
      public void setReader(Reader reader) throws IOException {
        numericTokenStream.reset();
        numericTokenStream.setIntValue(toInteger(reader));
      }
    };
  }

}
