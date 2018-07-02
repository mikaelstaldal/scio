/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders

import java.io.{InputStream, OutputStream}
import scala.annotation.implicitNotFound
import org.apache.beam.sdk.coders.{CustomCoder, Coder => BCoder, KvCoder}
import com.spotify.scio.ScioContext
import scala.reflect.ClassTag

@implicitNotFound("""
Cannot find a Coder instance for type:

  >> ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have a Coder instance in scope. Here are
  some debugging hints:
    - Make sure you imported com.spotify.scio.coders.Implicits._
    - If you can't annotate the definition, you can also generate a Coder using
        implicit val someClassCoder = com.spotify.scio.coders.Implicits.gen[SomeCaseClass]
    - For Option types, ensure that a Coder instance is in scope for the non-Option version.
    - For List and Seq types, ensure that a Coder instance is in scope for a single element.
    - You can check that an instance exists for Coder in the REPL or in your code:
        scala> Coder[Foo]
    And find the missing instance and construct it as needed.
""")
sealed trait Coder[T] extends Serializable
final case class Beam[T] private (beam: BCoder[T]) extends Coder[T]
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T]
final case class Transform[A, B] private (c: Coder[A], f: BCoder[A] => Coder[B]) extends Coder[B]

// final case class SerializableCoderLocker[A](@transient b: BCoder[A]) extends CustomCoder[A] {
//   val coder = com.twitter.chill.Externalizer(b)
//   def encode(value: A, os: OutputStream): Unit =
//     coder.get.encode(value, os)
//   def decode(is: InputStream): A =
//     coder.get.decode(is)
// }

sealed trait CoderGrammar {
  import org.apache.beam.sdk.coders.CoderRegistry
  import org.apache.beam.sdk.options.PipelineOptions
  import org.apache.beam.sdk.options.PipelineOptionsFactory

  def clean[T](w: BCoder[T]) =
    com.spotify.scio.util.ClosureCleaner.clean(w).asInstanceOf[BCoder[T]]

  def beam[T](beam: BCoder[T]) =
    Beam(beam)
  def fallback[T](implicit ct: ClassTag[T]) =
    Fallback[T](ct)
  def transform[A, B](c: Coder[A])(f: BCoder[A] => Coder[B]) =
    Transform(c, f)

  def beam[T](sc: ScioContext, c: Coder[T]): BCoder[T] =
    beam(sc.pipeline.getCoderRegistry, sc.options, c)

  def beamWithDefault[T](
    coder: Coder[T],
    r: CoderRegistry = CoderRegistry.createDefault(),
    o: PipelineOptions = PipelineOptionsFactory.create()) =
      beam(r, o, coder)

  final def beam[T](r: CoderRegistry, o: PipelineOptions, c: Coder[T]): BCoder[T] = {
    c match {
      case Beam(c) => c
      case Fallback(ct) =>
        com.spotify.scio.Implicits.RichCoderRegistry(r)
          .getScalaCoder[T](o)(ct)
      case Transform(c, f) =>
        val u = f(beam(r, o, c))
        beam(r, o, u)
    }
  }
}

sealed trait AtomCoders {
  import org.apache.beam.sdk.coders.{ Coder => BCoder, _}
  import Coder.beam
  implicit def byteCoder: Coder[Byte] = beam(ByteCoder.of().asInstanceOf[BCoder[Byte]])
  implicit def byteArrayCoder: Coder[Array[Byte]] = beam(ByteArrayCoder.of())
  implicit def bytebufferCoder: Coder[java.nio.ByteBuffer] = beam(???)
  implicit def stringCoder: Coder[String] = beam(StringUtf8Coder.of())
  implicit def intCoder: Coder[Int] = beam(VarIntCoder.of().asInstanceOf[BCoder[Int]])
  implicit def doubleCoder: Coder[Double] = beam(DoubleCoder.of().asInstanceOf[BCoder[Double]])
  implicit def floatCoder: Coder[Float] = beam(FloatCoder.of().asInstanceOf[BCoder[Float]])
  implicit def unitCoder: Coder[Unit] = beam(UnitCoder)
  implicit def nothingCoder: Coder[Nothing] = beam[Nothing](NothingCoder)
  implicit def booleanCoder: Coder[Boolean] = beam(BooleanCoder.of().asInstanceOf[BCoder[Boolean]])
  implicit def longCoder: Coder[Long] = beam(BigEndianLongCoder.of().asInstanceOf[BCoder[Long]])
  implicit def bigdecimalCoder: Coder[BigDecimal] = ???
}

final object Coder extends CoderGrammar with AtomCoders /*with TupleCoders*/ {
  // TODO: better error message
  // @deprecated("""
  //   No implicit coder found for type ${T}. Using a fallback coder.
  //   Most types should be supported out of the box by simply importing `com.spotify.scio.coders.Implicits._`.
  //   If a type is not supported, consider implementing your own implicit com.spotify.scio.coders.Coder for this type:

  //     class MyTypeCoder extends Coder[MyType] {
  //       def decode(in: InputStream): MyType = ???
  //       def encode(ts: MyType, out: OutputStream): Unit = ???
  //     }
  //     implicit def myTypeCoder: Coder[MyType] =
  //       new MyTypeCoder
  //   """, since="0.6.0")
  @deprecated("TODO: message", since="0.6.0")
  implicit def implicitFallback[T: ClassTag](implicit lp: shapeless.LowPriority): Coder[T] =
    Coder.fallback[T]

  import org.apache.beam.sdk.values.KV
  def kvCoder[K, V](ctx: ScioContext)(implicit k: Coder[K], v: Coder[V]): KvCoder[K, V] =
    KvCoder.of(Coder.beam(ctx, Coder[K]), Coder.beam(ctx, Coder[V]))

  def apply[T](implicit c: Coder[T]): Coder[T] = c
}