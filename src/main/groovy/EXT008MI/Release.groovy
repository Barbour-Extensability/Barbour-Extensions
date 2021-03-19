package EXT008MI

/**
 * README
 * This extension is being used in Mongoose
 *
 * Name: EXT008MI.Release
 * Description: Release pick list
 * Date	      Changed By           	          Description
 * 20201127   Hazel Anne Dimaculangan         Release pick list
 */
 
 import java.time.LocalDate;
 import java.time.LocalTime;
 import java.time.format.DateTimeFormatter;
 
public class Release extends ExtendM3Transaction {
  private final MIAPI mi;
  private final LoggerAPI logger;
  private final MICallerAPI miCaller;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  
  private String inDLIX, inPLSX, inTEAM, inPICK, inSEEQ, inZDEV, inDOVA, inPISS, inPLRI;
  private String XXCONO;
  private String message = "";
  private int currentDate, currentTime;
  
  public Release(MIAPI mi, LoggerAPI logger, MICallerAPI miCaller, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi;
    this.logger = logger;
    this.miCaller = miCaller;
    this.database = database;
    this.program = program;
  }
  
  public void main() {
    Map<String,String> apiResp;
    
    // - Get input values
    inDLIX = mi.inData.get("DLIX") == null ? "" : mi.inData.get("DLIX").trim();
    inPLSX = mi.inData.get("PLSX") == null ? "" : mi.inData.get("PLSX").trim();
    inTEAM = mi.inData.get("TEAM") == null ? "" : mi.inData.get("TEAM").trim();
    inPICK = mi.inData.get("PICK") == null ? "" : mi.inData.get("PICK").trim();
    inSEEQ = mi.inData.get("SEEQ") == null ? "" : mi.inData.get("SEEQ").trim();
    inZDEV = mi.inData.get("ZDEV") == null ? "" : mi.inData.get("ZDEV").trim();
    inDOVA = mi.inData.get("DOVA") == null ? "" : mi.inData.get("DOVA").trim();
    inPISS = mi.inData.get("PISS") == null ? "" : mi.inData.get("PISS").trim();
    inPLRI = mi.inData.get("PLRI") == null ? "" : mi.inData.get("PLRI").trim();
    
    // - Set company
    XXCONO = program.LDAZD.CONO.toString();
    
    // - Validate required inputs
    if("".equals(inDLIX)){
      mi.error("Delivery number is mandatory");
    }
    
    if("".equals(inPLSX)){
      mi.error("Picking list suffix is mandatory");
    }
    
    // - Set date and time formatters
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("hhmmss");
    
    // - Get current date and time
    currentDate = dateFormatter.format(LocalDate.now()).toInteger();
    currentTime = timeFormatter.format(LocalTime.now()).toInteger();
    
    // - Execute MWS420MI.UpdPickHead
    def updPickHeadParams = ["DLIX": inDLIX, "PLSX": inPLSX, "TEAM": inTEAM, "PICK": inPICK, "SEEQ": inSEEQ, "DEV": inZDEV, "DOVA": inDOVA];
    Closure<?> updPickHeadHandler = {Map<String,String> response ->
      apiResp = response;
      if(response.error){
        mi.error(response.errorMessage);
      }
    }
    miCaller.call("MWS420MI", "UpdPickHead", updPickHeadParams, updPickHeadHandler);
    
    if(apiResp == null){
      updMHPICH();
      mi.outData.put("ZMSG", message);
      mi.write();
    }
  }
  
  private void updMHPICH(){
    DBAction query = database.table("MHPICH").index("00").selectAllFields().build();
    DBContainer MHPICH = query.createContainer();
    MHPICH.setInt("PICONO", XXCONO.toInteger());
    MHPICH.setLong("PIDLIX", inDLIX.toLong());
    MHPICH.setInt("PIPLSX", inPLSX.toInteger());
    Closure<?> updateRecord = {LockedResult result -> 
      if("30".equals(result.get("PIPISS").toString().trim())){
        if(!"".equals(inPISS)){
          result.set("PIPISS", inPISS);
        }
        result.set("PIPLRI", "");
        message = "Update performed.";
        result.setInt("PIARLE", currentDate);
        result.setInt("PIARLF", currentTime);
        result.update();
      } else {
        mi.error("Status is greater than 30.");
        return;
      }
    }
    if(!query.readLock(MHPICH, updateRecord)){
      mi.error("Record does not exist")
    }
  }
}
