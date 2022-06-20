terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.6.0"
    }
  }
}

data "databricks_current_user" "me" {}
data "databricks_spark_version" "latest" {}
data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_job" "this" {
  name = "autoloader_trigger_once"
  git_source {
    url = "https://github.com/rafaelvp-db/delta-auto-loader-examples"
    branch = "main"
    provider = "github"
  }
  
  job_cluster {
    job_cluster_key = "j"
    new_cluster {
      num_workers   = 2
      spark_version = data.databricks_spark_version.latest.id
      node_type_id  = data.databricks_node_type.smallest.id
    }
  }

    task {
      task_key = "bronze"

      notebook_task {
        notebook_path = "notebooks/python/trigger_once/bronze"
      }

      job_cluster_key = "j"
    }

  task {
    task_key = "silver"

    depends_on {
      task_key = "bronze"
    }

    notebook_task {
      notebook_path = "notebooks/python/trigger_once/silver"
    }

    job_cluster_key = "j"
  }

  task {
    task_key = "gold"

    depends_on {
      task_key = "silver"
    }

    notebook_task {
      notebook_path = "notebooks/python/trigger_once/gold"
      
    }

    job_cluster_key = "j"

  }
  

}

output "job_url" {
  value = databricks_job.this.url
}

