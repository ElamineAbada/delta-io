/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.internal

import org.scalatest.funsuite.AnyFunSuite

class S3LogStoreUtilTest extends AnyFunSuite {
  test("keyBefore") {
    assert("a" == S3LogStoreUtil.keyBefore("b"))
    assert("aa/aa" == S3LogStoreUtil.keyBefore("aa/ab"))
    assert(Seq(1.toByte, 1.toByte)
       == S3LogStoreUtil.keyBefore(new String(Seq(1.toByte, 2.toByte).toArray)).getBytes.toList)
  }

  test("keyBefore with emojis") {
    assert("♥a" == S3LogStoreUtil.keyBefore("♥b"))
  }

  test("keyBefore with zero bytes") {
    assert("abc" == S3LogStoreUtil.keyBefore("abc\u0000"))
  }

  test("keyBefore with empty key") {
    assert(null == S3LogStoreUtil.keyBefore(""))
  }
}
