# delta-auto-loader-examples
Some examples using **Databricks Auto Loader, Spark Structured Streaming, Databricks Workflows and Terraform.**

<img src="https://res.cloudinary.com/hevo/images/f_auto,q_auto/v1636944359/hevo-learn/Databricks-Autoloader-Streaming-and-Batch-Loads/Databricks-Autoloader-Streaming-and-Batch-Loads.png?_i=AA" />

# What's part of this repo?

| What                            | Where                | Details                                                                                                |
|---------------------------------|----------------------|--------------------------------------------------------------------------------------------------------|
| Delta Auto Loader Examples      | `notebooks/autoloader` | Contains example notebooks of Delta Auto Loader                                                        |
| Medallion Architecture Examples | `notebooks/medallion`  | Sample notebooks for implementing a simple medallion architecture                                      |
| Databricks Workflows            | `terraform`            | Terraform scripts for declaring a Databricks Workflows job to orchestrate the notebooks from medallion |


# Getting Started

* To quickly get started, you can clone this repo into your Databricks Workspace by following the steps [here](https://docs.databricks.com/repos/set-up-git-integration.html)
* Once the repo is integrated with your workspace, you can view and run all the notebooks in the `notebooks` folder.

# High Level Architecture

<img src="https://github.com/rafaelvp-db/delta-auto-loader-examples/blob/master/img/Auto%20Loader@2x.png?raw=true">

# Additional Resources

### Documentation

* [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
* [Databricks Terraform Provider](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs)

### YouTube

[![Auto Loader Video](https://img.youtube.com/vi/8a38Fv9cpd8/0.jpg)](https://www.youtube.com/watch?v=8a38Fv9cpd8)
