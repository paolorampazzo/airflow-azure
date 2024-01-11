from kubernetes.client import models as k8s
from kubernetes.client import V1LocalObjectReference

def define_k8s_specs(memory_limit=None, memory_request='2Gi',
                  cpu_request='100m', other_specs={}):

  if not memory_limit:  
    memory_limit = memory_request

  kube_exec_config_special = {
              "pod_override": k8s.V1Pod(
                  spec=k8s.V1PodSpec(
                      containers=[
                          k8s.V1Container(
                              name="base",
                              resources=k8s.V1ResourceRequirements(
                                  requests={"memory": memory_request, "cpu": cpu_request},
                                  limits={"memory": memory_limit}
                              )
                          ),
                      ], 
                      
                  )
              )
          }

  config = {**kube_exec_config_special, **other_specs}

  return config