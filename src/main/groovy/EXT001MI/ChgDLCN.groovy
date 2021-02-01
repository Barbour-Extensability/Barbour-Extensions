/**
 * README
 * This extension is being triggered by CMS045/CMS041
 *
 * Name: EXT001MI.ChgDLCN
 * Description: Change MHDISH/DLCN upon Create
 * Date	      Changed By           	Description
 * 20200805   Joana Marie Sabino    Create extension change MHDISH/DLCN upon Create
 */
public class ChgDLCN extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final MICallerAPI miCaller;
  private final LoggerAPI logger;
  private final ProgramAPI program;

  //Input Fields
  private String iINOU;
  private String iDLIX;
  private String iDLCN;

  public ChgDLCN(MIAPI mi, DatabaseAPI database, MICallerAPI miCaller, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi;
    this.database = database;
    this.miCaller = miCaller;
    this.logger = logger;
    this.program = program;
  }

  public void main() {
    iINOU = mi.inData.get("INOU") == null? "": mi.inData.get("INOU").trim();
    iDLIX = mi.inData.get("DLIX") == null? "": mi.inData.get("DLIX").trim();
    iDLCN = mi.inData.get("DLCN") == null? "": mi.inData.get("DLCN").trim();

    DBAction MHDISH_query = database.table("MHDISH")
      .index("00")
      .build();
    DBContainer MHDISH_update = database.createContainer("MHDISH");
    MHDISH_update.set("OQCONO", program.LDAZD.CONO);
    MHDISH_update.setInt("OQINOU", iINOU.toInteger());
    MHDISH_update.setLong("OQDLIX", iDLIX.toLong());

    Closure<?> MHDISH_update_ResultHandler  = { LockedResult result ->
      result.setInt("OQDLCN", 2);
      result.update();
    };

    MHDISH_query.readLock(MHDISH_update, MHDISH_update_ResultHandler);
  }
}
