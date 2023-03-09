/**
 * README
 * This extension is being triggered by CMS045/CMS041
 *
 * Name: EXT001MI.UpdDLCN
 * Description: Update MHDISH/DLCN
 * Date	      Changed By           	          Description
 * 20200805   Joana Marie Sabino              Create extension update MHDISH/DLCN
 * 20201210   Hazel Anne Dimaculangan         Change conditions for RASN
 */
public class UpdDLCN extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final MICallerAPI miCaller;
  private final LoggerAPI logger;
  private final ProgramAPI program;

  //Input Fields
  private String iINOU;
  private String iDLIX;
  private String iDLCN;

  public UpdDLCN(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi;
    this.database = database;
    this.miCaller = miCaller;
    this.logger = logger;
    this.program = program;
  }

  public void main() {
    iINOU = mi.inData.get("INOU").trim();
    iDLIX = mi.inData.get("DLIX").trim();
    iDLCN = mi.inData.get("DLCN").trim();

    DBAction query = database.table("MHDISH")
      .index("00")
      .selection("OQRASN","OQDLCN", "OQPGRS")
      .build();

    DBContainer container = query.getContainer();
    container = query.getContainer();
    container.set("OQCONO", program.LDAZD.CONO)
    container.setInt("OQINOU", iINOU.toInteger());
    container.setLong("OQDLIX", iDLIX.toLong());

    if (!query.read(container)) {
      mi.error("Direction" + iINOU + "and Delivery Number " + iDLIX + " does not exists");
      return;
    }else{
      if(container.get("OQRASN") == "STD" || container.get("OQRASN") == "N/A" || container.get("OQRASN") == "VAS"){
        executeUpdateDLCN(0);
      }else if (container.get("OQRASN") == "BOOKIN" || container.get("OQRASN") == "BOOKIN/VAS" || container.get("OQRASN") == "PRE-BOOK" || container.get("OQRASN") == "PREBOOKVAS"){
        executeUpdateDLCN(2);
      }
    }
  }

  private void executeUpdateDLCN(Integer xxdlcn){
    DBAction MHDISH_query = database.table("MHDISH")
      .index("00")
      .build();
    DBContainer MHDISH_update = database.createContainer("MHDISH");
    MHDISH_update.set("OQCONO", program.LDAZD.CONO);
    MHDISH_update.setInt("OQINOU", iINOU.toInteger());
    MHDISH_update.setLong("OQDLIX", iDLIX.toLong());

    Closure<?> MHDISH_update_ResultHandler  = { LockedResult result ->
      result.setInt("OQDLCN", xxdlcn);
      result.update();
    };

    MHDISH_query.readLock(MHDISH_update, MHDISH_update_ResultHandler);
  }
}
