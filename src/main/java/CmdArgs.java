import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.BooleanOptionHandler;

/**
 * Created by user on 2017/3/8.
 */
public class CmdArgs {
    @Option(name="-cores",usage="core")
    public static int numCores = 8;

    @Option(name="-memory",usage="memory")
    public static int numMemory = 32;

    @Option(name="-disk",usage="disks")
    public static int numDisks = 1;

    @Option(name="-k",usage="hbase(T/F)")
    public static String isHbaseInstalled="F";
}
