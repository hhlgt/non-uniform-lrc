import os

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)
cluster_number = 30
datanode_number_per_cluster = 10
datanode_port_start = 17600
cluster_id_start = 0
iftest = False
vc_per_pc = 3

proxy_ip_list = [
    ["10.0.0.2",41406],
    ["10.0.0.2",41506],
    ["10.0.0.2",41606],
    ["10.0.0.3",41406],
    ["10.0.0.3",41506],
    ["10.0.0.3",41606],
    ["10.0.0.5",41406],
    ["10.0.0.5",41506],
    ["10.0.0.5",41606],
    ["10.0.0.6",41406],
    ["10.0.0.6",41506],
    ["10.0.0.6",41606],
    ["10.0.0.7",41406],
    ["10.0.0.7",41506],
    ["10.0.0.7",41606],
    ["10.0.0.8",41406],
    ["10.0.0.8",41506],
    ["10.0.0.8",41606],
    ["10.0.0.9",41406],
    ["10.0.0.9",41506],
    ["10.0.0.9",41606],
    ["10.0.0.10",41406],
    ["10.0.0.10",41506],
    ["10.0.0.10",41606],
    ["10.0.0.11",41406],
    ["10.0.0.11",41506],
    ["10.0.0.11",41606],
    ["10.0.0.12",41406],
    ["10.0.0.12",41506],
    ["10.0.0.12",41606]
]
coordinator_ip = "10.0.0.4"
networkcore_address = "10.0.0.18:17550"

if iftest:
    proxy_ip_list = [
        ["0.0.0.0",50005],
        ["0.0.0.0",50035],
        ["0.0.0.0",50065],
        ["0.0.0.0",50095],
        ["0.0.0.0",50125],
        ["0.0.0.0",50155],
        ["0.0.0.0",50185],
        ["0.0.0.0",50215],
        ["0.0.0.0",50245],
        ["0.0.0.0",50275],
        ["0.0.0.0",50305],
        ["0.0.0.0",50335],
        ["0.0.0.0",50365],
        ["0.0.0.0",50395],
        ["0.0.0.0",50425],
        ["0.0.0.0",50455],
        ["0.0.0.0",50485],
        ["0.0.0.0",50515],
        ["0.0.0.0",50545],
        ["0.0.0.0",50575],
        ["0.0.0.0",50605],
        ["0.0.0.0",50635],
        ["0.0.0.0",50665],
        ["0.0.0.0",50695],
        ["0.0.0.0",50725],
        ["0.0.0.0",50755],
        ["0.0.0.0",50785],
        ["0.0.0.0",50815],
        ["0.0.0.0",50845],
        ["0.0.0.0",50875]
    ]
    coordinator_ip = "0.0.0.0"
    networkcore_address = "0.0.0.0:17500"

proxy_num = len(proxy_ip_list)

cluster_informtion = {}
def generate_cluster_info_dict():
    for i in range(cluster_number):
        new_cluster = {}
        
        new_cluster["proxy"] = proxy_ip_list[i][0]+":"+str(proxy_ip_list[i][1])
        datanode_list = []
        for j in range(datanode_number_per_cluster):
            # port = datanode_port_start + j
            # if iftest:
            port = datanode_port_start + i*100 + j
            datanode_list.append([proxy_ip_list[i][0], port])
        new_cluster["datanode"] = datanode_list
        cluster_informtion[i] = new_cluster
            
def generate_run_proxy_datanode_file():
    file_name = parent_path + '/run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for cluster_id in cluster_informtion.keys():
            print("cluster_id",cluster_id)
            for each_datanode in cluster_informtion[cluster_id]["datanode"]:
                f.write("./project/cmake/build/run_datanode "+str(each_datanode[0])+":"+str(each_datanode[1])+"\n")
            f.write("\n") 
        f.write("./project/cmake/build/run_datanode "+networkcore_address+"\n")
        f.write("\n")
        for i in range(cluster_number):
            proxy_ip_port = proxy_ip_list[i]
            f.write("./project/cmake/build/run_proxy "+str(proxy_ip_port[0])+":"+str(proxy_ip_port[1])+" "+networkcore_address+" "+coordinator_ip+"\n")   
        f.write("\n")

def generater_cluster_information_xml():
    file_name = parent_path + '/project/config/clusterInformation.xml'
    import xml.etree.ElementTree as ET
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_informtion.keys():
        cluster = ET.SubElement(root, 'cluster', {'id': str(cluster_id), 'proxy': cluster_informtion[cluster_id]["proxy"]})
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index,each_datanode in enumerate(cluster_informtion[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {'uri': str(each_datanode[0])+":"+str(each_datanode[1])})
            #datanode.text = '\n\t\t\t'
            if index == len(cluster_informtion[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_informtion)-1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    #root.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)
            
def cluster_generate_run_proxy_datanode_file(ip, port, i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for each_datanode in cluster_informtion[0]["datanode"]:
            f.write("./project/cmake/build/run_datanode "+ip+":"+str(each_datanode[1])+"\n")
        f.write("\n") 
        f.write("./project/cmake/build/run_proxy "+ip+":"+str(port)+" "+networkcore_address+" "+coordinator_ip+"\n")   
        f.write("\n")

def cluster_generate_run_proxy_datanode_file_v2(i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            ip = proxy_ip_list[j][0]
            port = proxy_ip_list[j][1]
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/cmake/build/run_datanode "+ip+":"+str(each_datanode[1])+"\n")
            f.write("\n") 
            f.write("./project/cmake/build/run_proxy "+ip+":"+str(port)+" "+networkcore_address+" "+coordinator_ip+"\n")   
            f.write("\n")

if __name__ == "__main__":
    generate_cluster_info_dict()
    generater_cluster_information_xml()
    if iftest:
        generate_run_proxy_datanode_file()
    else:
        cnt = 0
        for i in range(cluster_number / vc_per_pc):
            cluster_generate_run_proxy_datanode_file_v2(i)
            cnt += 1
        file_name = parent_path + '/run_cluster_sh/' + str(cnt) +'/cluster_run_proxy_datanode.sh'
        with open(file_name, 'w') as f:
            f.write("./project/cmake/build/run_datanode "+networkcore_address+"\n")
            f.write("\n")
        cnt += 1
    