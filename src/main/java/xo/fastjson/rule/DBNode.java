package xo.fastjson.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DBNode {
        private String       type;
        private List<String> ipList = new ArrayList<>();
        private String       ip;
        private List<String> portList = new ArrayList<>();
        private String       port;
        private String       instance;
        private String       user;
        private String       password;
        private String       uuid;
        private String       catalog;
        private String       name;
        private String inst;
        
        public DBNode() {
            
        }

        public String getType() {
            return type;
        }

        public String getIp() {
            return ip;
        }

        public String getPort() {
            return port;
        }

        public String getInstance() {
            return instance;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public String getUuid() {
            return uuid;
        }
        
        public void setDBNode(String type, String ip, String port, String instance, String user, 
                String password, String catalog ,String name,String uuid) {
            this.ip   = ip;
            this.type = type;
            this.port = port;
            this.instance = instance;
            this.user     = user;
            this.password = password;
            this.catalog  = catalog;
            this.name = name;
            this.uuid = uuid;
            //setInst(ip,port);
            dealIpList(ip);
        }

        private void dealIpList(String ip) {
            if (ip.contains(",")) {
                ipList = Arrays.stream(ip.split("\\s*,\\s*"))
                        .map(String::trim).collect(Collectors.toList());
                this.ip = ipList.get(0);
            } else {
                this.ip = ip;
            }

            this.inst = this.ip + ":" + this.port;
        }

        public String getName()
        {
            return name;
        }



        public void setName(String name)
        {
            this.name = name;
        }

        public String getCatalog()
        {
            return catalog;
        }

        public void setCatlog(String catalog)
        {
            this.catalog = catalog;
        }

        public String getInst()
        {
            return inst;
        }

        public void setInst(String ip,String port)
        {
            this.inst = ip+":"+port;
        }

    public List<String> getIpList() {
        return ipList;
    }

    public void setInst(String inst)
    {
        this.inst = inst;
        
    }
}
