package EXT008MI

/**
 * README
 * This extension is being used in Mongoose
 *
 * Name: EXT008MI.ReleaseWave
 * Description: Release wave pick list
 * Date	      Changed By           	          Description
 * 20201219   Edelisa Dacillo                 Release wave pick list
 * 20201222   Hazel Anne Dimaculangan         Update MITALO table
 */

import java.text.SimpleDateFormat;
import java.time.LocalDate

public class ReleaseWave extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final LoggerAPI logger;
  private final ProgramAPI program;
  private final IonAPI ion;
  private final MICallerAPI miCaller;
  
  private long iDLIX;
  private int iPLSX;
  private String iPLRI;
  private Map listDLIX = new HashMap();
  int currentCompany
  private String gplri
  private String padPLRI
  
  public ReleaseWave(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, IonAPI ion, MICallerAPI miCaller) {
    this.mi = mi;
    this.database = database;
    this.logger = logger;
    this.program = program;
    this.ion = ion;
    this.miCaller = miCaller;
  }
  
  public void main() {
    iDLIX = mi.in.get("DLIX");
    iPLSX = mi.in.get("PLSX");
    iPLRI = mi.in.get("PLRI");
 
    int xplrilen = 10 - iPLRI.trim().length()
    padPLRI = iPLRI.trim()
    padPLRI = padPLRI.padLeft(10, ' ')
    iPLRI = padPLRI    

    currentCompany = (Integer)program.getLDAZD().CONO
    DBAction query = database.table("MHPICH").index("00")
    .selection("PICONO", "PIDLIX", "PIPLSX", "PIPLRI", "PIWHLO", "PISLTP", "PIPISE")
    .build()
    DBContainer container = query.getContainer()
    container.set("PICONO", currentCompany)
    container.set("PIDLIX", iDLIX)
    container.set("PIPLSX", iPLSX)
    boolean hasrecord = false;
    String plri = ""
    String whlo = ""
    String sltp = ""
    String pise = ""
    if (query.read(container)) {
      plri = container.get("PIPLRI").toString()
      whlo = container.get("PIWHLO").toString().trim()
      sltp = container.get("PISLTP").toString().trim()
      pise = container.get("PIPISE").toString().trim()
    }
  
    if (!iPLRI.isEmpty()) {
      gplri = iPLRI
      updateMHPICH();
      updateMITALO(whlo);
      addMITDPR(iPLRI, whlo, sltp, pise)
    }
  }
  
  def getRandomNum() {
    Random rnd = new Random();
       String xrand = "";
       int number = 0;
       for(int i=0; i<18; i++) {
        number = rnd.nextInt(9);
        xrand += String.format("%01d", number);
       }
       return xrand;
  }
  
  def addMITDPR(String plri, String whlo, String sltp, String pise) {
    String xrand = getRandomNum()
    
    DBAction query = database.table("MITDPR").index("00").selection("MPBJNO").build()
    DBContainer container = query.getContainer()
    container.set("MPBJNO", xrand)
    container.set("MPCONO", currentCompany)
    container.set("MPRIDI", iDLIX)
    container.set("MPPLSX", iPLSX)
    container.set("MPSLTP", sltp)
    container.set("MPPISE", pise)
    container.set("MPPLRI", plri)
    if (query.read(container)) {
      
    } else {
      def date = new Date();
      def sdf = new SimpleDateFormat("yyyyMMdd");
      String currentDate = sdf.format(date);
      sdf = new SimpleDateFormat("HHmmss");
      String currentTime = sdf.format(date);
      
      DBAction ActionMITDPR = database.table("MITDPR").build();
	    DBContainer MITDPR = ActionMITDPR.getContainer();
	    MITDPR.set("MPBJNO", xrand)
	    MITDPR.set("MPCONO", currentCompany)
	    MITDPR.set("MPRIDI", iDLIX)
	    MITDPR.set("MPPLSX", iPLSX)
	    MITDPR.set("MPWHLO", whlo)
	    MITDPR.set("MPSLTP", sltp)
	    MITDPR.set("MPPGNM", "MWS437")
	    MITDPR.set("MPPLRI", plri)
	    MITDPR.set("MPPISE", pise)
	    MITDPR.set("MPUSID", program.getUser())
	    MITDPR.set("MPFPGM", "MWS420MI")
	    MITDPR.set("MPDOVA", "40")
	    MITDPR.set("MPPRTF", "MWS437PF")
	    ActionMITDPR.insert(MITDPR, RecordExists);
	    R972(xrand, whlo)
	  }
    
  }
  
  Closure RecordExists = {
    mi.error("Record already exists");
  }
  
  def void R972(String xrand, String whlo) {
    def date = new Date();
    def sdf = new SimpleDateFormat("yyyyMMdd");
    String currentDate = sdf.format(date);
    sdf = new SimpleDateFormat("HHmmss");
    String currentTime = sdf.format(date);
    DBAction ActionMW972 = database.table("MMW972").build()
    DBContainer MW972 = ActionMW972.getContainer();
    MW972.set("J2CONO",currentCompany);
    MW972.set("J2WHLO",whlo);
    MW972.set("J2BJNO",xrand);
    MW972.set("J2PGNM", "MWS437");
    MW972.set("J2RGDT", currentDate.toInteger());
    MW972.set("J2RGTM", currentTime.toInteger());
    MW972.set("J2CHID", program.getUser());
    ActionMW972.insert(MW972, RecordExists);
  }
   
  def updateMHPICH() {
    DBAction query = database.table("MHPICH").index("00").selection("PICONO", "PIDLIX", "PIPLSX").build()
    DBContainer container = query.getContainer()
    container.set("PICONO", currentCompany)
    container.set("PIDLIX", iDLIX)
    container.set("PIPLSX", iPLSX)
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("PIPISS", "40")
      lockedResult.set("PIPLRI", gplri)
      lockedResult.update()
    }
    query.readLock(container, updateCallBack)
  }
  
  def updateMITALO(String whlo){
    DBAction query = database.table("MITALO").index("30").selection("MQPLRI").build();
    DBContainer ITALO = query.createContainer();
    ITALO.set("MQCONO", currentCompany);
    ITALO.set("MQRIDI", iDLIX);
    ITALO.set("MQPLSX", iPLSX);
    ITALO.set("MQWHLO", whlo);
    Closure<?> update = {LockedResult result->
      result.set("MQPLRI", iPLRI);
      result.update();
      logger.info("Successfully updated MITALO.")
    }
    query.readAllLock(ITALO, 4, update);
  }
}
