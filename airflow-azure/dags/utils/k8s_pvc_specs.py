from kubernetes.client import models as k8s
from kubernetes.client import V1LocalObjectReference

def define_k8s_specs(claim_name, memory_limit=None, memory_request='300Mi',
                  cpu_request='100m'):

  config = {
              "pod_override": k8s.V1Pod(
                  spec=k8s.V1PodSpec(
                      containers=[
                          k8s.V1Container(
                              name="base",
                              volume_mounts=[k8s.V1VolumeMount(name=claim_name,
                                                               mount_path="/mnt/mydata")],
                              resources=k8s.V1ResourceRequirements(
                                  requests={"memory": memory_request, "cpu": cpu_request},
                                  limits={"memory": memory_limit}
                              )
                          ),
                      ], 
                      volumes = [k8s.V1Volume(name=claim_name, 
                                             persistent_volume_claim = k8s.V1PersistentVolumeClaimVolumeSource(
                        claim_name=claim_name
                    ))]
                      
                  )
              )
          }


  return config