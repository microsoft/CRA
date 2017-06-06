# Common Runtime for Applications

Common Runtime for Applications (CRA) is a software layer (library) that makes it easy to create and deploy distributed dataflow-style applications on top of resource managers such as Kubernetes, YARN, and stand-alone cluster execution. Currently, we support stand-alone execution (just deploy an .exe on every machine in your cluster) as well as execution in a Kubernetes/Docker environment.

After you clone the source code, check out the wiki at https://github.com/Microsoft/CRA/wiki for instruction on building CRA and running your first sample distributed application - either locally or in a Kubernetes (Windows) cluster using a Docker image of CRA. We have provided detailed step-by-step instructions for accomplishing this in the wiki. We show how to do this on [Azure Container Service](https://azure.microsoft.com/en-us/services/container-service/) (ACS), but CRA should work on any other Kubernetes cluster as well. 

# Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
