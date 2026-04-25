[producers]
${vm_producers_public_ip} ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/benchmark_aws ansible_host_key_checking=False vm_private_ip=${vm_producers_ip}

[broker]
${vm_broker_public_ip} ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/benchmark_aws ansible_host_key_checking=False vm_private_ip=${vm_broker_ip}

[compute]
${vm_compute_public_ip} ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/benchmark_aws ansible_host_key_checking=False vm_private_ip=${vm_compute_ip}

%{ if enable_brazil_compute ~}
[compute_brazil]
${vm_compute_brazil_public_ip} ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/benchmark_aws ansible_host_key_checking=False vm_private_ip=${vm_compute_brazil_ip}

%{ endif ~}

[sink]
${vm_sink_public_ip} ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/benchmark_aws ansible_host_key_checking=False vm_private_ip=${vm_sink_ip}

[all:vars]
ansible_python_interpreter=/usr/bin/python3
vm_producers_ip=${vm_producers_ip}
vm_broker_ip=${vm_broker_ip}
vm_compute_ip=${vm_compute_ip}
vm_sink_ip=${vm_sink_ip}
vm_compute_brazil_ip=${vm_compute_brazil_ip}
vm_producers_public_ip=${vm_producers_public_ip}
vm_broker_public_ip=${vm_broker_public_ip}
vm_compute_public_ip=${vm_compute_public_ip}
vm_sink_public_ip=${vm_sink_public_ip}
vm_compute_brazil_public_ip=${vm_compute_brazil_public_ip}
