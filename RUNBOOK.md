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

## 2. Software, on every node

Each machine needs Java 11 and Python 3.10.

```bash
git clone https://github.com/VivianSobers/distributed-search-engine.git
cd distributed-search-engine
bash scripts/install_deps.sh
```

Edit `config/cluster.env` and replace the four `NODE*_IP` placeholders with the ZeroTier IPs. Every machine needs the same values, and the node scripts refuse to run while a placeholder remains.

On Node 1, download the corpus from the course page to `data/full/phase-0/pages.jsonl` and `data/full/phase-1/changes.json`.

On Node 4, create the serving directory:

```bash
sudo mkdir -p /serving && sudo chown "$USER" /serving
```

The node scripts are bash scripts. Run them with `bash scripts/<name>.sh` from any directory.

## 3. Start the cluster

Run these in order:

| Order | Node | Command |
|---|---|---|
| 1 | Node 2 | `bash scripts/node2_start.sh` |
| 2 | Node 3 | `bash scripts/node3_start.sh` |
| 3 | Node 4 | `bash scripts/node4_serve.sh`, and leave it running |

`node2_start.sh` formats the NameNode only the first time. Check the cluster from Node 2 or Node 3:

```bash
source scripts/lib.sh
hdfs dfsadmin -report | grep 'Live datanodes'
```

It should report 2 live DataNodes. The Spark master page at `http://<node3-ip>:8080` should list 2 workers.
