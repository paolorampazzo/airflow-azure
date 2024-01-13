from kubernetes.client import models as k8s
from kubernetes.client import V1LocalObjectReference

def define_k8s_specs(claim_name = '', memory_limit=None, memory_request='300Mi',
                  cpu_request='100m', node_selector = {}):
  
    config = {
              "pod_override": k8s.V1Pod(
                  spec=k8s.V1PodSpec(                
                      containers=[
                          k8s.V1Container(
                              name="base",
                            #   volume_mounts=[k8s.V1VolumeMount(name=claim_name,
                            #                                    mount_path="/mnt/mydata")],
                              resources=k8s.V1ResourceRequirements(requests={"memory": memory_request, "cpu": cpu_request},
                                #   limits={"memory": memory_limit}
                              ),
                          ),
                      ], 
                    #   volumes = [k8s.V1Volume(name=claim_name, 
                    #                          persistent_volume_claim = k8s.V1PersistentVolumeClaimVolumeSource(
                    #     claim_name=claim_name
                    # ))]
                      
                  )
              )
          }

    if claim_name:
        volumes = [k8s.V1Volume(name=claim_name, 
                                             persistent_volume_claim = k8s.V1PersistentVolumeClaimVolumeSource(
                        claim_name=claim_name
                    ))]
        
        volume_mounts=[k8s.V1VolumeMount(name=claim_name,
                                                               mount_path="/mnt/mydata")],
        
        config['pod_override'].spec.volumes = volumes
        config['pod_override'].spec.containers[0].volume_mounts = volume_mounts



    if len(node_selector):
        key = node_selector['key']
        values = node_selector['values']
        
        

        affinity = k8s.V1NodeAffinity(required_during_scheduling_ignored_during_execution = \
                                        [k8s.V1NodeSelectorTerm(match_expressions= [
                                            k8s.V1NodeSelectorRequirement(key=key,
                                                                        operator='In',
                                                                        values= values)
                                        ])])    
        
        
        config['pod_override'].spec.affinity = affinity 
    return config