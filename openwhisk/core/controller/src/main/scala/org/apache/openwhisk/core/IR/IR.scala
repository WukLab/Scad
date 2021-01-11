package org.apache.openwhisk.core.IR

// Definations of internal structures, you can replace this with the existing ones


object IRContext {

  trait _ObjectID {
    def generateUniqueID : _ObjectID
  }
  type _ObjectName = String
  type _ObjectRuntime = String

  // Object runtime.
  type ObjectRuntime = _ObjectRuntime
  type ObjectID = Option[_ObjectID]
  type ObjectName = Option[_ObjectName]
  // Reference to Openwhisk Function
  type ImageRef = String

  type ObjectType = String

  type ResourceType = String
  object ResourceLocality extends Enumeration {
    type ResourceLocality = Value
    val Near, Far = Value
  }

  import ResourceLocality._
  case class ResourceSpec (
    resourceType : ResourceType,
    resourceAmount : Option[Float],
    resourceLocality: ResourceLocality
  )

  object ImageRef {
    def empty = ""
  }

  case class ParallelismControlDomain (
    name: ObjectName, instance: ObjectID
  ) {
    def createInstance(id : ObjectID) = this.copy(instance = id)
    def isMatch(template : ParallelismControlDomain) = {
      val instanceMatch = for { x <- instance; y <- template.instance } yield x == y
      val nameMatch = for { x <- name; y <- template.name } yield x == y
      nameMatch.getOrElse(true) && instanceMatch.getOrElse(true)
    }
  }
  object ParallelismControlDomain {
    def empty = ParallelismControlDomain(Option.empty, Option.empty)
    def apply(name : _ObjectName): ParallelismControlDomain = apply(Option(name), Option.empty)
  }

  case class ObjectReference (
    application: ParallelismControlDomain,
    function: ParallelismControlDomain,
    name: ParallelismControlDomain
  ) {
    def isMatch(t : ObjectReference) =
      application.isMatch(t.application) && function.isMatch(t.function) && name.isMatch(t.name)
  }
  object ObjectReference {
    def static(app : String, func: String, obj: String) = ObjectReference(
      ParallelismControlDomain(app), ParallelismControlDomain(func), ParallelismControlDomain(obj))
    def staticObject(name : String) = ObjectReference(
      ParallelismControlDomain.empty, ParallelismControlDomain.empty, ParallelismControlDomain(name))
    def staticFunction(name : String) = ObjectReference(
      ParallelismControlDomain.empty, ParallelismControlDomain(name), ParallelismControlDomain.empty)
  }

  case class IRObjectRelationship (
    // We may want to change this incoming reference to accept openwhisk trigger?
    dependency : (Seq[ObjectReference], Seq[ObjectReference]),
    corunning : Seq[ObjectReference],
    // We may add more relations here
  )
  object IRObjectRelationship {
    def empty = IRObjectRelationship((Seq.empty, Seq.empty), Seq.empty)
  }

  // Extra infomation for scheduler
  case class ObjectProperty (
    objectType: ObjectType = "",
    longRunning: Boolean = false,
    dynamic: Boolean = false,

    concurrencyLimitFunction : Int = 0,
    concurrencyLimitApplication : Int = 0,
  )
  object ObjectProperty {
    def empty = ObjectProperty()
  }
}

import IRContext._

// IR Objects
case class IRObject (
  name: _ObjectName,
  resources: ResourceSpec,
  runtime : ObjectRuntime,


  property: ObjectProperty = ObjectProperty.empty,
  relationship: IRObjectRelationship = IRObjectRelationship.empty,
  corunning : Seq[ObjectReference] = Seq.empty,
  image: ImageRef = ImageRef.empty,

  // Here, we can add more helper members
  function: Option[IRFunction] = Option.empty,
  // application: IRApplication,
  // etc
) {
  def reference = ObjectReference.staticObject(name)
  def fullReference : Option[ObjectReference] = for {
    func <- function
    app <- func.application
  } yield ObjectReference.static(app.name, func.name, name)
}

// Function here is for parallelism control,
case class IRFunction (
  name: _ObjectName,
  objects: Seq[IRObject],

  // Here, we can add more helper members
  application: Option[IRApplication] = Option.empty,
) {
  def reference = ObjectReference.staticFunction(name)

  def addObject(obj : IRObject) =
    copy(objects = objects :+ obj.copy(function = Option(this)))

  def findObject(t : ObjectReference) : Option[IRObject] = objects.find(_.reference.isMatch(t))
}

// Object within one application can have  each other
case class IRApplication (
  name: _ObjectName,
  functions: Seq[IRFunction]
) {
  def addFunction(func : IRFunction) =
    copy(functions = functions :+ func.copy(application = Option(this)))

  def findObject(t : ObjectReference) : Option[IRObject] =
    functions
      .find(_.reference.isMatch(t))
      .flatMap(_.findObject(t))

  // get all objects
  def objects = functions.flatMap(_.objects)
}
