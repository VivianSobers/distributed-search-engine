# Four-node runbook

These steps run the project across four machines. They have not yet been rehearsed on four separate machines. The single-machine rehearsal is `bash scripts/local_e2e.sh`.

## 1. Network

Put all four machines on one ZeroTier network and note each machine's ZeroTier IP with `sudo zerotier-cli listnetworks`.

Spark and HDFS use many ports, so trust the ZeroTier interface on each machine:

| System | Command |
|---|---|
| Ubuntu (ufw) | `sudo ufw allow in on <zerotier-interface>` |
| Fedora (firewalld) | `sudo firewall-cmd --permanent --zone=trusted --add-interface=<zerotier-interface> && sudo firewall-cmd --reload` |

Find the interface name with `ip link`. It starts with `zt`.
