import org.kohsuke.args4j.CmdLineParser;

/**
 * Created by user on 2017/3/30.
 */


public class OptimizeSetting4Yarn {
    private static int getMinContainerSize(int memory) {
        int retVal =0;
        if (memory <= 4) {
            retVal = 256;
        }
        else if(memory <= 8) {
            retVal = 512;
        }else if(memory <= 24) {
            retVal = 1024;
        }else {
            retVal = 2048;
        }
        return retVal;
    }

    private static int getReservedHBaseMem(int memory) {
        int returnValue = 0;
        switch (memory){
            case 4:
                returnValue =1;
                break;
            case 8:
            case 16:
                returnValue =2;
                break;
            case 24:
                returnValue =4;
                break;
            case 48:
                returnValue =6;
                break;
            case 64:
            case 72:
                returnValue =8;
                break;
            case 96:
                returnValue =16;
                break;
            case 128:
                returnValue =24;
                break;
            case 256:
                returnValue =32;
                break;
            case 512:
                returnValue =64;
                break;
            default:
                if(memory<=4){
                    returnValue =1;
                }else if(memory >=512){
                    returnValue = 64;
                }else{
                    returnValue = 2;
                }
        }

        return returnValue;
    }

    private static int getReservedStackMemory(int memory){
        int returnValue = 0;
        switch (memory){
            case 4:
                returnValue =1;
                break;
            case 8:
            case 16:
                returnValue =2;
                break;
            case 24:
                returnValue =4;
                break;
            case 48:
                returnValue =6;
                break;
            case 64:
            case 72:
                returnValue =8;
                break;
            case 96:
                returnValue =12;
                break;
            case 128:
                returnValue =24;
                break;
            case 256:
                returnValue =32;
                break;
            case 512:
                returnValue =64;
                break;
            default:
                if(memory<=4){
                    returnValue =1;
                }else if(memory >=512){
                    returnValue = 64;
                }else{
                    returnValue = 1;
                }
        }

        return returnValue;
    }

    public static void main(String[] args) {
        try {
            CmdArgs cmdArgs = new CmdArgs();
            CmdLineParser parser = new CmdLineParser(cmdArgs);
            parser.parseArgument(args);

            System.out.println("orginal cores:"+cmdArgs.numCores);
            System.out.println("orginal memory(GB):"+cmdArgs.numMemory);
            System.out.println("orginal disk:"+cmdArgs.numDisks);
            System.out.println("orginal isHaseInstalled:"+cmdArgs.isHbaseInstalled);

            int memory = cmdArgs.numMemory;
            int minContainerSize = getMinContainerSize(memory);
            int reservedStackMemory = getReservedStackMemory(memory);
            int reservedHBaseMemory = 0;
            if(cmdArgs.isHbaseInstalled.equals("T")){
                reservedHBaseMemory = getReservedHBaseMem(memory);
            }

            int reservedMem = reservedStackMemory + reservedHBaseMemory;
            int usableMem = memory - reservedMem;
            memory -= reservedMem;
            if (memory < 2) {
                memory = 2;
                reservedMem = Math.max(0, memory - reservedMem);
            }

            memory *= 1024;

            int containers = Math.min(2 * cmdArgs.numCores,
                    Math.min((int)Math.ceil(1.8 * cmdArgs.numDisks),memory/minContainerSize));
            if (containers <= 2) {
                containers = 3;
            }

            int container_ram =  Math.abs(memory/containers);
            if (container_ram > 1024) {
                container_ram = (int) Math.floor(container_ram / 512) * 512;
            }

            System.out.println("Profile: cores=" + cmdArgs.numCores + " memory=" + memory + "MB"
                    + " reserved=" + reservedMem + "GB" + " usableMem="
                    + usableMem + "GB" + " disks=" + cmdArgs.numDisks);
            System.out.println("Num Container=" + containers);
            System.out.println("Container Ram=" + container_ram + "MB");
            System.out.println("Used Ram=" + (int)containers*container_ram/1024 + "GB");
            System.out.println("Unused Ram=" + reservedMem + "GB");
            System.out.println("yarn.scheduler.minimum-allocation-mb=" + container_ram);
            System.out.println("yarn.scheduler.maximum-allocation-mb=" + containers*container_ram);
            System.out.println("yarn.nodemanager.resource.memory-mb=" + containers*container_ram);
            int map_memory = container_ram;
            int reduce_memory = (container_ram <= 2048)?2*container_ram:container_ram;
            int am_memory = Math.max(map_memory, reduce_memory);
            System.out.println("mapreduce.map.memory.mb=" + map_memory);
            System.out.println("mapreduce.map.java.opts=-Xmx" + (int)(0.8 * map_memory) +"m");
            System.out.println("mapreduce.reduce.memory.mb=" + reduce_memory);
            System.out.println("mapreduce.reduce.java.opts=-Xmx" + (int)(0.8 * reduce_memory) + "m");
            System.out.println("yarn.app.mapreduce.am.resource.mb=" + am_memory);
            System.out.println("yarn.app.mapreduce.am.command-opts=-Xmx" + (int)(0.8*am_memory) + "m");
            System.out.println("mapreduce.task.io.sort.mb=" + (int)(0.4 * map_memory));
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}

