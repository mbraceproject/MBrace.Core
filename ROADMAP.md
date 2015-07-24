# Road map for MBrace.Core

## Towards MBrace.Core 1.0

* The current focus is on finalizing the 1.0 release of MBrace.Core and MBrace.Azure

* A major piece of work is a runtime refactoring, taking fabric independent parts of MBrace.Azure into a new component MBrace.Runtime.Core.

The [open issues for the milestone release are here](https://github.com/mbraceproject/MBrace.Core/milestones/1.0%20Release).

Ways you can contribute to the 1.0 release:

* [MBrace.CSharp rewrite](https://github.com/mbraceproject/MBrace.Core/issues/22). This is a rewrite and redesign of the C#
  API to access MBrace.Core.

* Improve the documentation, particularly [the website content here](https://github.com/mbraceproject/mbrace-docs/tree/master/docs/content)

* Improve [the programming model tutorial samples here](https://github.com/mbraceproject/MBrace.StarterKit/tree/master/azure/HandsOnTutorial)

* Improve the documentation, particularly the XML documentation throughout the code

* Add examples and tutorials showing how MBrace.Core and MBrace.Azure can be used with industry relevant 
  data sources such as databases
  
* Help target MBrace.Core and MBrace.Runtime.Core at new platform targets including Amazon AWS.

* Help document how MBrace can be used in more incremental and/or streaming scenarios. For example, document
  how CloudQueue inputs can be used to CloudFlow and show examples of this working.

* Help document how outputs from MBrace can be consumed by web source, including in streaming and/ web socket scenarios.

## Beyond 1.0 - Future work you can help contribute to 

* Help target MBrace.Core and MBrace.Runtime.Core at new platform targets such as Mesos, Docker-based fabrics and YARN.

