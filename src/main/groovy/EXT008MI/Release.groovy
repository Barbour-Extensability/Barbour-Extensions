/**
 * README
 * This extension is being used in Mongoose
 *
 * Name: EXT008MI.Release
 * Description: Release pick list
 * Date	      Changed By           	          Description
 * 20201127   Hazel Anne Dimaculangan         Release pick list
 * 20211012   E. Dacillo                      Add reference to MWMNGPCS
 */


import java.math.RoundingMode
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

  private String OQWHLO = "";
  private String OQDPOL = "";
  private String OQPIST = "";
  private int OQTTYP = 0;
  private int EDPCSP = 0;
  private int EDCPTM = 0;
  private int EDPRTA = 0;
  private int EDPIRL = 0;
  private String EDDOVA = "";
  private boolean packageCheck = false;
  private boolean isDone = false;
  private int XCPRIO = 0;
  private String XCPC40 = "";
  private String XCPC30 = "";
  private String XCPC20 = "";
  private String XCPC10 = "";
  private String XCOBV4 = "";
  private String XCOBV3 = "";
  private String XCOBV2 = "";
  private String XCOBV1 = "";
  private String XOBVAL = "";
  private String XOBFLD = "";
  private String PIWHLO = "";
  private String PISLTP = "";
  private String PIPISE = "";
  private int MCPIRL = 0;
  private int MCMXPP = 0;
  private double MCMXPW = 0D;
  private double MCMXPV = 0D;
  private double MCMXPT = 0D;
  private int MCMXPL = 0;
  private int MCMXWP = 0;
  private double MCMXWW = 0D;
  private double MCMXWV = 0D;
  private double MCMXWT = 0D;
  private int MCMXWL = 0;
  private boolean checkForSplit = false;
  private boolean checkNoOfPackages = false;
  private boolean checkItemWeight = false;
  private boolean checkItemVolume = false;
  private boolean checkPickTime = false;
  private boolean checkPickLines = false;
  private int PAKCNT = 0;
  private int LINWCT = 0;
  private int LINCNT = 0;
  private int TOTCNT = 0;
  private int XXNPLL = 0;
  private double PKGRWE = 0D;
  private double PKVOL3 = 0D;
  private double TIMCNT = 0D;
  private double TIMWCT = 0D;
  private double PKDLQT = 0D;
  private double XXGRWE = 0D;
  private double XXVOL3 = 0D;
  private double XXNUM = 0D;
  private int XXPCSI = 0;
  private boolean fromspltNoOfPac = false;
  private int MMCAWP = 0;
  private double MMGRWE = 0D;
  private double MMVOL3 = 0D;
  private String MMGRTS = "";
  private long save_PLRN = 0;
  private boolean picklistPrintJob = false;
  private boolean splitPerformed = false;
  private boolean lineUpdate = false;
  private boolean breakOnLine = false;
  private boolean breakOnPackage = false;
  private boolean breakOnTime = false;
  private boolean breakOnWeight = false;
  private boolean breakOnVolume = false;
  private boolean updatePackInfo = false;

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
        mi.error("Status is not 30.");
        return;
      }
    }
    if(!query.readLock(MHPICH, updateRecord)){
      mi.error("Record does not exist")
    } else {
      rtvDLIX(inDLIX.toLong());
      rtvDPOL();
      logger.debug("Release: OQDPOL=${OQDPOL}/OQPIST=${OQPIST}/OQTTYP=${OQTTYP}/EDPCSP=${EDPCSP}/EDCPTM=${EDCPTM}/EDPRTA=${EDPRTA}/EDPIRL=${EDPIRL}/EDDOVA=${EDDOVA}")
      if (EDPCSP == 1) {
        RPCS();
      }
    }
  }

  /**
   * rtvDLIX - Get information from MHDISH
   */
  def void rtvDLIX(long iDLIX) {
    OQWHLO = "";
    OQDPOL = "";
    OQTTYP = 0;
    OQPIST = "";
    DBAction query = database.table("MHDISH")
      .index("00")
      .selection("OQWHLO", "OQDPOL", "OQTTYP", "OQPIST")
      .build();
    DBContainer container = query.getContainer();
    container.set("OQCONO", XXCONO.toInteger());
    container.set("OQINOU", 1);
    container.set("OQDLIX", iDLIX);
    if (query.read(container)) {
      OQWHLO = container.getString("OQWHLO").trim();
      OQDPOL = container.getString("OQDPOL").trim();
      OQTTYP = container.getInt("OQTTYP");
      OQPIST = container.getString("OQPIST");
    }
  }

  /**
   * rtvDPOL - Get information from MHDIPO
   */
  def void rtvDPOL() {
    EDPCSP = 0;
    EDCPTM = 0;
    EDPRTA = 0;
    EDPIRL = 0;
    EDDOVA = "";
    DBAction query = database.table("MHDIPO")
      .index("00")
      .selection("EDPCSP", "EDCPTM", "EDPRTA", "EDPIRL", "EDDOVA")
      .build();
    DBContainer container = query.getContainer();
    container.set("EDCONO", XXCONO.toInteger());
    container.set("EDDPOL", OQDPOL);
    container.set("EDWHLO", OQWHLO);
    if (query.read(container)) {
      EDPCSP = container.getInt("EDPCSP");
      EDCPTM = container.getInt("EDCPTM");
      EDPRTA = container.getInt("EDPRTA");
      EDPIRL = container.getInt("EDPIRL");
      EDDOVA = container.getString("EDDOVA");
    } else {
      container.set("EDWHLO", "");
      if (query.read(container)) {
        EDPCSP = container.getInt("EDPCSP");
        EDCPTM = container.getInt("EDCPTM");
        EDPRTA = container.getInt("EDPRTA");
        EDPIRL = container.getInt("EDPIRL");
        EDDOVA = container.getString("EDDOVA");
      }
    }
  }

  /**
   *    RPCS - Package Based Picking - perform split
   */
  public void RPCS() {
    //=========================================
    //MWMNGPCS - RUPD
    // If return/indelivery
    if (OQTTYP == 40) {
      return;
    }
    // Retrieve Pick List header
    DBAction query = database.table("MHPICH")
      .index("00")
      .selectAllFields()
      .build();
    DBContainer MHPICH = query.getContainer();
    MHPICH.setInt("PICONO", XXCONO.toInteger());
    MHPICH.setLong("PIDLIX", inDLIX.toLong());
    MHPICH.setInt("PIPLSX", inPLSX.toInteger());
    if (!query.read(MHPICH)) {
      return;
    }
    // Check picking status on the dispatch header. No package break if not started
    packageCheck = false;
    if (OQPIST.toInteger() >= 30) {
      packageCheck = true;
    } else {
      if (OQPIST == 20) {
        packageCheck = true;
        List<DBContainer> lstMITALO30 = readMITALO30(MHPICH);
        for (DBContainer recMITALO30: lstMITALO30) {
          double PAQT = recMITALO30.getDouble("MQPAQT");
          double TRQT = recMITALO30.getDouble("MQTRQT");
          if (PAQT != TRQT) {
            packageCheck = false;
            break;
          }
        }
      }
    }
    // Retrieve capacity parameters
    rtvCapacityParam(MHPICH);

    // Check return information
    checkForSplit = false;
    checkNoOfPackages = false;
    checkItemWeight = false;
    checkItemVolume = false;
    checkPickTime = false;
    checkPickLines = false;
    // Work variables
    PAKCNT = 0;
    LINCNT = 0;
    TOTCNT = 0;
    XXNPLL = MHPICH.getInt("PINPLL");
    PKGRWE = 0D;
    PKVOL3 = 0D;
    TIMCNT = 0D;
    PKDLQT = 0D;
    XXPCSI = 0;
    // Line split
    if (MCMXPL != 0) {
      checkPickLines = true;
      checkForSplit = true;
    }
    // Packages split (if packing started)
    if (MCMXPP != 0 && packageCheck) {
      checkNoOfPackages = true;
      checkForSplit = true;
    }
    // Weight split
    if (MCMXPW != 0) {
      checkItemWeight = true;
      checkForSplit = true;
    }
    // Volume split
    if (MCMXPV != 0) {
      checkItemVolume = true;
      checkForSplit = true;
    }
    // Pick Time split
    if (MCMXPT != 0 && EDCPTM == 1) {
      checkPickTime = true;
      checkForSplit = true;
    }
    logger.debug("release: checkForSplit=${checkForSplit}/packageCheck=${packageCheck}/XXNPLL=${XXNPLL}")
    // Checks to be made - determine whether package based or allocation based
    if (checkForSplit) {
      if (packageCheck) {
        spltNoOfPac(MHPICH);
        // Delete last written record
        Closure<?> deleteMMWPCS = { LockedResult lockedResult ->
          lockedResult.delete();
        }
        DBAction queryDelMMWPCS = database.table("MMWPCS")
          .index("00")
          .build();
        DBContainer containerDelMMWPCS = queryDelMMWPCS.getContainer();
        containerDelMMWPCS.set("PCCONO", MHPICH.getInt("PICONO"));
        containerDelMMWPCS.set("PCDLIX", Long.parseLong(MHPICH.get("PIDLIX").toString().trim()));
        queryDelMMWPCS.readAllLock(containerDelMMWPCS, 2, deleteMMWPCS);
        fromspltNoOfPac = false;
        if (EDPIRL != 2) {
          CPSPLT(MHPICH);
        }
      } else {
        CASPLT(MHPICH);
      }
    } else {
      if (EDPRTA == 1) {
        PRPIC(MHPICH);
      }
    }
  }

  /**
   * readMITALO30 - read records from MITALO
   * @return
   */
  def List<DBContainer> readMITALO30(DBContainer recMHPICH) {
    List<DBContainer> containers = new ArrayList();
    def queryMITALO = database.table("MITALO")
      .index("30")
      .selectAllFields()
      .build();
    def containerMITALO = queryMITALO.createContainer();
    containerMITALO.set("MQCONO", recMHPICH.getInt("PICONO"));
    containerMITALO.setLong("MQRIDI", recMHPICH.getLong("PIDLIX"));
    containerMITALO.set("MQPLSX", recMHPICH.getInt("PIPLSX"));
    queryMITALO.readAll(containerMITALO, 3,  { DBContainer record ->
      containers.add(record.createCopy());
    });
    return containers;
  }

  /**
   * readMITALO30_MQPLSX - read records from MITALO
   * @return
   */
  def List<DBContainer> readMITALO30_MQPLSX(DBContainer recMHPICH, int orgITALOPLSX) {
    List<DBContainer> containers = new ArrayList();
    def queryMITALO = database.table("MITALO")
      .index("30")
      .selectAllFields()
      .build();
    def containerMITALO = queryMITALO.createContainer();
    containerMITALO.set("MQCONO", recMHPICH.getInt("PICONO"));
    containerMITALO.setLong("MQRIDI", recMHPICH.getLong("PIDLIX"));
    containerMITALO.set("MQPLSX", orgITALOPLSX);
    queryMITALO.readAll(containerMITALO, 3,  { DBContainer record ->
      containers.add(record.createCopy());
    });
    return containers;
  }

  /**
   * rtvCapacityParam - simulate MWRTVPCA
   * @param recMHPICH
   * @return
   */
  def rtvCapacityParam(DBContainer recMHPICH) {
    PIWHLO = recMHPICH.getString("PIWHLO").trim();
    PISLTP = recMHPICH.getString("PISLTP").trim();
    PIPISE = recMHPICH.getString("PIPISE").trim();
    RRTV(recMHPICH);
  }

  /**
   *    RRTV   - Retrieve Picking capacity
   */
  def void RRTV(DBContainer recMHPICH) {
    String C3STAT = "";
    DBAction queryCROBJC = database.table("CROBJC")
      .index("00")
      .selectAllFields()
      .build();
    DBContainer containerCROBJC = queryCROBJC.getContainer();
    containerCROBJC.set("C3CONO", recMHPICH.getInt("PICONO"));
    containerCROBJC.set("C3PGNM", "MWS175");
    containerCROBJC.set("C3OBV1", recMHPICH.getString("PIWHLO").trim());
    containerCROBJC.set("C3OBV2", "");
    containerCROBJC.set("C3OBV3", "");
    if (queryCROBJC.read(containerCROBJC)) {
      C3STAT = containerCROBJC.getString("C3STAT").trim();
      //   Check matrix
      if (C3STAT == "20") {
        isDone = false;
        CHKMTX(containerCROBJC);
      }
    } else {
      return;
    }
  }

  /**
   *    CHKMTX - Check Picking capacity selection matrix for match
   */
  def void CHKMTX(DBContainer recCROBJC) {
    //   Check sel. matrix prio.  0
    if (recCROBJC.getString("C3PC01").trim() != "") {
      XCPRIO = 0;
      XCPC10 = recCROBJC.getString("C3PC01").trim();
      XCPC20 = recCROBJC.getString("C3PCO2").trim();
      XCPC30 = recCROBJC.getString("C3PCO3").trim();
      XCPC40 = recCROBJC.getString("C3PCO4").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 1
    if (!isDone && recCROBJC.getString("C3PC11").trim() != "") {
      XCPRIO = 1;
      XCPC10 = recCROBJC.getString("C3PC11").trim();
      XCPC20 = recCROBJC.getString("C3PC12").trim();
      XCPC30 = recCROBJC.getString("C3PC13").trim();
      XCPC40 = recCROBJC.getString("C3PC14").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 2
    if (!isDone && recCROBJC.getString("C3PC21").trim() != "") {
      XCPRIO = 2;
      XCPC10 = recCROBJC.getString("C3PC21").trim();
      XCPC20 = recCROBJC.getString("C3PC22").trim();
      XCPC30 = recCROBJC.getString("C3PC23").trim();
      XCPC40 = recCROBJC.getString("C3PC24").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 3
    if (!isDone && recCROBJC.getString("C3PC31").trim() != "") {
      XCPRIO = 3;
      XCPC10 = recCROBJC.getString("C3PC31").trim();
      XCPC20 = recCROBJC.getString("C3PC32").trim();
      XCPC30 = recCROBJC.getString("C3PC33").trim();
      XCPC40 = recCROBJC.getString("C3PC34").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 4
    if (!isDone && recCROBJC.getString("C3PC41").trim() != "") {
      XCPRIO = 4;
      XCPC10 = recCROBJC.getString("C3PC41").trim();
      XCPC20 = recCROBJC.getString("C3PC42").trim();
      XCPC30 = recCROBJC.getString("C3PC43").trim();
      XCPC40 = recCROBJC.getString("C3PC44").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 5
    if (!isDone && recCROBJC.getString("C3PC51").trim() != "") {
      XCPRIO = 5;
      XCPC10 = recCROBJC.getString("C3PC51").trim();
      XCPC20 = recCROBJC.getString("C3PC52").trim();
      XCPC30 = recCROBJC.getString("C3PC53").trim();
      XCPC40 = recCROBJC.getString("C3PC54").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 6
    if (!isDone && recCROBJC.getString("C3PC61").trim() != "") {
      XCPRIO = 6;
      XCPC10 = recCROBJC.getString("C3PC61").trim();
      XCPC20 = recCROBJC.getString("C3PC62").trim();
      XCPC30 = recCROBJC.getString("C3PC63").trim();
      XCPC40 = recCROBJC.getString("C3PC64").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 7
    if (!isDone && recCROBJC.getString("C3PC71").trim() != "") {
      XCPRIO = 7;
      XCPC10 = recCROBJC.getString("C3PC71").trim();
      XCPC20 = recCROBJC.getString("C3PC72").trim();
      XCPC30 = recCROBJC.getString("C3PC73").trim();;
      XCPC40 = recCROBJC.getString("C3PC74").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 8
    if (!isDone && recCROBJC.getString("C3PC81").trim() != "") {
      XCPRIO = 8;
      XCPC10 = recCROBJC.getString("C3PC81").trim();
      XCPC20 = recCROBJC.getString("C3PC82").trim();
      XCPC30 = recCROBJC.getString("C3PC83").trim();
      XCPC40 = recCROBJC.getString("C3PC84").trim();
      CLPMTX();
    }
    //   Check sel. matrix prio. 9
    if (!isDone && recCROBJC.getString("C3PC91").trim() != "") {
      XCPRIO = 9;
      XCPC10 = recCROBJC.getString("C3PC91").trim();
      XCPC20 = recCROBJC.getString("C3PC92").trim();
      XCPC30 = recCROBJC.getString("C3PC93").trim();
      XCPC40 = recCROBJC.getString("C3PC94").trim();
      CLPMTX();
    }
  }

  /**
   *    CLPMTX - Find Picking capacity selection matrix
   */
  def boolean CLPMTX() {
    XCOBV1 = "";
    XCOBV2 = "";
    XCOBV3 = "";
    XCOBV4 = "";
    if (XCPC10 != "") {
      XOBFLD = XCPC10;
      SETOBV();
      XCOBV1 = XOBVAL;
    }
    if (XCPC20 != "") {
      XOBFLD = XCPC20;
      SETOBV();
      XCOBV2 = XOBVAL;
    }
    if (XCPC30 != "") {
      XOBFLD = XCPC30;
      SETOBV();
      XCOBV3 = XOBVAL;
    }
    if (XCPC40 != "") {
      XOBFLD = XCPC40;
      SETOBV();
      XCOBV4 = XOBVAL;
    }

    DBAction queryMPICAP = database.table("MPICAP")
      .index("00")
      .selection("MCWHLO", "MCPIRL", "MCMXPP", "MCMXPW", "MCMXPV","MCMXPT","MCMXPL", "MCMXWP", "MCMXWW", "MCMXWV", "MCMXWT", "MCMXWL")
      .build();
    DBContainer containerMPICAP = queryMPICAP.getContainer();
    containerMPICAP.set("MCCONO", XXCONO.toInteger());
    containerMPICAP.set("MCWHLO", PIWHLO);
    containerMPICAP.set("MCPRIO", XCPRIO);
    containerMPICAP.set("MCOBV1", XCOBV1);
    containerMPICAP.set("MCOBV2", XCOBV2);
    containerMPICAP.set("MCOBV3", XCOBV3);
    containerMPICAP.set("MCOBV4", XCOBV4);
    boolean hasRecord = queryMPICAP.read(containerMPICAP);
    if (!hasRecord) {
      containerMPICAP.set("MCOBV4", "");
      hasRecord = queryMPICAP.read(containerMPICAP);
    }
    if (!hasRecord) {
      containerMPICAP.set("MCOBV3", "");
      containerMPICAP.set("MCOBV4", "");
      hasRecord = queryMPICAP.read(containerMPICAP);
    }
    if (!hasRecord) {
      containerMPICAP.set("MCOBV2", "");
      containerMPICAP.set("MCOBV3", "");
      containerMPICAP.set("MCOBV4", "");
      hasRecord = queryMPICAP.read(containerMPICAP);
    }
    if (hasRecord) {
      // setting Pick reporting level
      MCPIRL = containerMPICAP.getInt("MCPIRL");
      // setting Pick capacity
      MCMXPP = containerMPICAP.getInt("MCMXPP");
      MCMXPW = containerMPICAP.getDouble("MCMXPW");
      MCMXPV = containerMPICAP.getDouble("MCMXPV");
      MCMXPT = containerMPICAP.getDouble("MCMXPT");
      MCMXPL = containerMPICAP.getInt("MCMXPL");
      // setting Wave capacity
      MCMXWP = containerMPICAP.getInt("MCMXWP");
      MCMXWW = containerMPICAP.getDouble("MCMXWW");
      MCMXWV = containerMPICAP.getDouble("MCMXWV");
      MCMXWT = containerMPICAP.getDouble("MCMXWT");
      MCMXWL = containerMPICAP.getInt("MCMXWL");
      isDone = true;
      logger.debug("release: MPICAPhasRecord=${hasRecord}/MCPIRL=${MCPIRL}/MCMXPP=${MCMXPP}/MCMXPW=${MCMXPW}/MCMXPV=${MCMXPV}/MCMXPT=${MCMXPT}/MCMXPL=${MCMXPL}");
      logger.debug("release: MPICAPhasRecord=${hasRecord}/MCMXWP=${MCMXWP}/MCMXWW=${MCMXWW}/MCMXWV=${MCMXWV}/MCMXPV=${MCMXPV}/MCMXWT=${MCMXWT}/MCMXWL=${MCMXWL}")
    }
    return true;
  }

  /**
   *    SETOBV - Set obj. values for loading platform matrix
   */
  def void SETOBV() {
    switch (0) {
      default:
        if (XOBFLD == "PIWHLO") {
          XOBVAL = PIWHLO;
          break;
        }
        if (XOBFLD == "PISLTP") {
          XOBVAL = PISLTP;
          break;
        }
        if (XOBFLD == "PIPISE") {
          XOBVAL = PIPISE;
          break;
        }
    }
  }

  /**
   *    spltNoOfPac  - Split based on number of packages
   */
  def void spltNoOfPac(DBContainer recMHPICH) {
    fromspltNoOfPac = true;
    long inPIDLIX = Long.parseLong(recMHPICH.get("PIDLIX").toString().trim());
    int inPIPLSX = recMHPICH.getInt("PIPLSX");
    String inPIPLRI = recMHPICH.getString("PIPLRI").trim();

    // Allocations
    int orgITALOPLSX = 0;
    String orgITALOPLRI = "";
    boolean updatePackInfo = false;
    int save_PLSX = 0;
    String save_PLRI = "";
    int PCPLSX = 0;
    String PCPLRI = "";
    int XXPLSX = 0;
    String XXPLRI = "";

    List<DBContainer> listMITALO30 = readMITALO30(recMHPICH);
    for (DBContainer recMITALO30: listMITALO30) {
      // save PLSX from MITALO
      orgITALOPLSX = recMITALO30.getInt("MQPLSX");
      // save PLRI from MITALO
      orgITALOPLRI = recMITALO30.getString("MQPLRI");
      // Adjust reporting number on MFTRND
      updatePackInfo = false;
      // Perform accumulations
      List<DBContainer> listMFTRND10 = readMFTRND10(recMITALO30);
      for (DBContainer recMFTRND : listMFTRND10) {
        save_PLSX = inPIPLSX;
        save_PLRI = inPIPLRI;
        DBAction queryMMWPCS = database.table("MMWPCS")
          .index("00")
          .selection("PCPANR", "PCPLRI", "PCPLSX")
          .build();
        DBContainer containerMMWPCS = queryMMWPCS.getContainer();
        containerMMWPCS.set("PCCONO", recMITALO30.getInt("MQCONO"));
        containerMMWPCS.set("PCDLIX", Long.parseLong(recMFTRND.get("O0DLIX").toString().trim()));
        containerMMWPCS.set("PCPANR", recMFTRND.getString("O0PANR"));
        containerMMWPCS.set("PCPLRI", inPIPLRI);
        containerMMWPCS.set("PCPLSX", inPIPLSX);
        // See if record exists for the package (DLIX and PANR)
        int recCountMMWPCS = queryMMWPCS.readAll(containerMMWPCS, 3, { DBContainer recordMMWPCS ->
          PCPLSX = recordMMWPCS.getInt("PCPLSX");
          PCPLRI = recordMMWPCS.getString("PCPLRI");
        });
        if (recCountMMWPCS == 0) {
          // Increase package number counter and create record with reference between package and picking list (and wave number)
          PAKCNT++;
          queryMMWPCS.insert(containerMMWPCS);
          // Check number of packages limit
          if (checkNoOfPackages &&
            PAKCNT > MCMXPP) {
            XXPCSI = 1;
            updatePackInfo = true;
            lineUpdate = true;
            PAKCNT--;
            RSPLIT(recMHPICH, recMITALO30);
            // Delete last written record
            Closure<?> deleteMMWPCS = { LockedResult lockedResult ->
              lockedResult.delete();
            }
            DBAction queryDelMMWPCS = database.table("MMWPCS")
              .index("00")
              .build();
            DBContainer containerDelMMWPCS = queryDelMMWPCS.getContainer();
            containerDelMMWPCS.set("PCCONO", recMITALO30.getInt("MQCONO"));
            containerDelMMWPCS.set("PCDLIX", Long.parseLong(recMFTRND.get("O0DLIX").toString().trim()));
            containerDelMMWPCS.set("PCPANR", recMFTRND.get("O0PANR").toString().trim());
            containerDelMMWPCS.set("PCPLRI", save_PLRI);
            containerDelMMWPCS.set("PCPLSX", save_PLSX);
            queryDelMMWPCS.readLock(containerDelMMWPCS, deleteMMWPCS);

            // Create record with a new picking list suffix (PLSX) and new wave number (PLRI)
            containerMMWPCS.set("PCPLRI", inPLRI);
            containerMMWPCS.set("PCPLSX", inPLSX);
            recCountMMWPCS = queryMMWPCS.readAll(containerMMWPCS, 3, { DBContainer recordMMWPCS ->
              PCPLSX = recordMMWPCS.getInt("PCPLSX");
              PCPLRI = recordMMWPCS.getString("PCPLRI");
            });
            if (recCountMMWPCS == 0) {
              queryMMWPCS.insert(containerMMWPCS);
            }
          }
        }
        // Compare picking list suffix and wave number from the MITALO record with that of the package reference in MMWPCS
        XXPLSX = PCPLSX;
        XXPLRI = PCPLRI;
        // If picking list suffix or the wave number is different on the MITALO record compared to the refernce in MMWPCS
        if (XXPLSX != orgITALOPLSX || XXPLRI != orgITALOPLRI) {
          getItemDtls(recMITALO30.getString("MQITNO").trim());
          long newPLRN = Long.parseLong(recMITALO30.get("MQPLRN").toString().trim());
          newPLRN = reduceQtyMITALO80(recMITALO30, recMFTRND, orgITALOPLSX);
          // Check if there exists a MITALO record with suffix equal to the suffix referenced for the package in MMWPCS
          // If it does add the quantity from MFTRND to that MITALO record and add wave number
          save_PLRN = Long.parseLong(recMITALO30.get("MQPLRN").toString().trim());
          newPLRN = addQtyMITALO80(recMITALO30, recMFTRND, XXPLSX, XXPLRI);
          // Change MITALO references by deleting the MFTRND record with reporting nunber from the current MITALO record
          processMFTRND(recMFTRND, save_PLRN, newPLRN);
        }
      }
      delAllocationLine(recMITALO30);
    }

    //Remove current picking list if all picking list lines moved to new picking list
    listMITALO30 = readMITALO30_MQPLSX(recMHPICH, orgITALOPLSX);
    if (listMITALO30.size() == 0) {
      delMHPICH(recMHPICH, orgITALOPLSX);
    } else {
      // Update last MHPICH
      if (splitPerformed || lineUpdate && LINCNT > 0) {
        Closure<?> updateMHPICH = { LockedResult lockedResult ->
          lockedResult.set("PINPLL", LINCNT);
          lockedResult.set("PIPLTM", TIMCNT);
          lockedResult.set("PILMDT", currentDate);
          lockedResult.set("PICHID", program.getUser());
          lockedResult.set("PICHNO", lockedResult.getInt("PICHNO") + 1);
          lockedResult.update();
        }
        DBAction queryMHPICH = database.table("MHPICH")
          .index("00")
          .build();
        DBContainer containerMHPICH = queryMHPICH.getContainer()
        containerMHPICH.set("PICONO", XXCONO.toInteger());
        containerMHPICH.set("PIDLIX", inPIDLIX);
        containerMHPICH.set("PIPLSX", inPIPLSX);
        queryMHPICH.readLock(containerMHPICH, updateMHPICH);
      }
      // Update last MHPICH - execute even if no split performed
      updLastMHPICH(recMHPICH);
    }
  }

  /**
   * readMFTRND10 - read MFTRND
   * @param recMITALO
   * @return
   */
  def List<DBContainer> readMFTRND10(DBContainer recMITALO) {
    List<DBContainer> containers = new ArrayList();
    def queryMFTRND = database.table("MFTRND")
      .index("10")
      .selection("O0DLIX", "O0PANR", "O0DLQT")
      .build();
    def containerMFTRND = queryMFTRND.createContainer();
    containerMFTRND.set("O0CONO", XXCONO.toInteger());
    containerMFTRND.set("O0WHLO", recMITALO.getString("MQWHLO"));
    containerMFTRND.set("O0DLIX", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMFTRND.set("O0RORC", recMITALO.getInt("MQTTYP") / 10);
    containerMFTRND.set("O0RIDN", recMITALO.getString("MQRIDN"));
    containerMFTRND.set("O0RIDL", recMITALO.getInt("MQRIDL"));
    containerMFTRND.set("O0RIDX", recMITALO.getInt("MQRIDX"));
    containerMFTRND.set("O0PLRN", Long.parseLong(recMITALO.get("MQPLRN").toString().trim()));
    queryMFTRND.readAll(containerMFTRND, 8, { DBContainer recordMFTRND ->
      containers.add(recordMFTRND.createCopy());
    });
    return containers;
  }

  /**
   * reduceQtyMITALO80 - Reduce qty on current MITALO record with portion from MFTRND
   * @param recMITALO
   * @param recMFTRND
   * @param orgITALOPLSX
   * @return
   */
  def long reduceQtyMITALO80(DBContainer recMITALO, DBContainer recMFTRND, int orgITALOPLSX) {
    long newPLRN = 0;
    // Reduce qty on current MITALO record with portion from MFTRND
    Closure<?> updateMITALO2 = { LockedResult lockedResult ->
      lockedResult.set("MQALQT", recMITALO.getDouble("MQALQT") - recMFTRND.getDouble("O0DLQT"));
      lockedResult.set("MQPAQT", recMITALO.getDouble("MQPAQT") - recMFTRND.getDouble("O0DLQT"));
      lockedResult.set("MQTRQT", recMITALO.getDouble("MQTRQT") - recMFTRND.getDouble("O0DLQT"));
      lockedResult.set("MQLMDT", currentDate);
      lockedResult.set("MQCHID", program.getUser());
      lockedResult.set("MQCHNO", lockedResult.getInt("MQCHNO") + 1);
      lockedResult.update();
      newPLRN = Long.parseLong(lockedResult.get("MQPLRN").toString().trim());
      if (recMITALO.getDouble("MQCAWE") != 0 && MMCAWP == 0) {
        RCAWE(recMITALO);
      }
    }
    DBAction queryMITALO2 = database.table("MITALO")
      .index("80")
      .build();
    DBContainer containerMITALO = queryMITALO2.getContainer();
    containerMITALO.set("MQCONO", XXCONO.toInteger())
    containerMITALO.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMITALO.set("MQPLSX", orgITALOPLSX);
    containerMITALO.set("MQCAMU", recMITALO.getString("MQCAMU"));
    containerMITALO.set("MQITNO", recMITALO.getString("MQITNO"));
    containerMITALO.set("MQWHLO", recMITALO.getString("MQWHLO"));
    containerMITALO.set("MQWHSL", recMITALO.getString("MQWHSL"));
    containerMITALO.set("MQBANO", recMITALO.getString("MQBANO"));
    containerMITALO.set("MQTTYP", recMITALO.getInt("MQTTYP"));
    containerMITALO.set("MQRIDN", recMITALO.getString("MQRIDN"));
    containerMITALO.set("MQRIDO", recMITALO.getInt("MQRIDO"));
    containerMITALO.set("MQRIDL", recMITALO.getInt("MQRIDL"));
    containerMITALO.set("MQRIDX", recMITALO.getInt("MQRIDX"));
    containerMITALO.set("MQSOFT", recMITALO.getInt("MQSOFT"));
    queryMITALO2.readLock(containerMITALO, updateMITALO2);
    return newPLRN;
  }


  /**
   * addQtyMITALO80 - Check if there exists a MITALO record with suffix equal to the suffix referenced for the package in MMWPCS
   *                  If it does add the quantity from MFTRND to that MITALO record and add wave number
   * @param recMITALO
   * @param recMFTRND
   * @param XXPLSX
   * @param XXPLRI
   * @return
   */
  def long addQtyMITALO80(DBContainer recMITALO, DBContainer recMFTRND, int XXPLSX, String XXPLRI) {
    long newPLRN = 0;
    // Check if there exists a MITALO record with suffix equal to the suffix referenced for the package in MMWPCS
    // If it does add the quantity from MFTRND to that MITALO record and add wave number
    Closure<?> updateMITALO3 = { LockedResult lockedResult ->
      // MITALO-record exist
      lockedResult.set("MQPLRI", XXPLRI);
      lockedResult.set("MQALQT", recMITALO.getDouble("MQALQT") + recMFTRND.getDouble("O0DLQT"));
      lockedResult.set("MQPAQT", recMITALO.getDouble("MQPAQT") + recMFTRND.getDouble("O0DLQT"));
      lockedResult.set("MQTRQT", recMITALO.getDouble("MQTRQT") + recMFTRND.getDouble("O0DLQT"));
      lockedResult.set("MQLMDT", currentDate);
      lockedResult.set("MQCHID", program.getUser());
      lockedResult.set("MQCHNO", lockedResult.getInt("MQCHNO") + 1);
      lockedResult.update();
      newPLRN = Long.parseLong(lockedResult.get("MQPLRN").toString().trim());
      if (recMITALO.getDouble("MQCAWE") != 0 && MMCAWP == 0) {
        RCAWE(recMITALO);
      }
    }

    DBAction queryMITALO2 = database.table("MITALO")
      .index("80")
      .build();
    DBContainer containerMITALO = queryMITALO2.getContainer();
    containerMITALO.set("MQCONO", XXCONO.toInteger())
    containerMITALO.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMITALO.set("MQPLSX", XXPLSX);
    containerMITALO.set("MQCAMU", recMITALO.getString("MQCAMU"));
    containerMITALO.set("MQITNO", recMITALO.getString("MQITNO"));
    containerMITALO.set("MQWHLO", recMITALO.getString("MQWHLO"));
    containerMITALO.set("MQWHSL", recMITALO.getString("MQWHSL"));
    containerMITALO.set("MQBANO", recMITALO.getString("MQBANO"));
    containerMITALO.set("MQTTYP", recMITALO.getInt("MQTTYP"));
    containerMITALO.set("MQRIDN", recMITALO.getString("MQRIDN"));
    containerMITALO.set("MQRIDO", recMITALO.getInt("MQRIDO"));
    containerMITALO.set("MQRIDL", recMITALO.getInt("MQRIDL"));
    containerMITALO.set("MQRIDX", recMITALO.getInt("MQRIDX"));
    containerMITALO.set("MQSOFT", recMITALO.getInt("MQSOFT"));
    queryMITALO2.readLock(containerMITALO, updateMITALO3);
    if (!queryMITALO2.readLock(containerMITALO, updateMITALO3)) {
      //If no MITALO record with suffix equal to the suffix referenced for the package in MMWPCS exists
      //then create a new MITALO-record with picking list suffix and wave number from the reference in MMWPCS
      save_PLRN = Long.parseLong(recMITALO.get("MQPLRN").toString().trim());
      containerMITALO.set("MQPLSX", XXPLSX);
      containerMITALO.set("MQPLRI", XXPLRI);
      containerMITALO.set("MQALQT", recMFTRND.getDouble("O0DLQT"));
      containerMITALO.set("MQPAQT", recMFTRND.getDouble("O0DLQT"));
      containerMITALO.set("MQTRQT", recMFTRND.getDouble("O0DLQT"));
      newPLRN = Long.parseLong(PLRN());
      containerMITALO.set("MQPLRN", newPLRN);
      queryMITALO2.insert(containerMITALO);
      if (recMITALO.getDouble("MQCAWE") != 0 && MMCAWP == 0) {
        containerMITALO.set("MQCAWE", RCAWE(recMITALO));
      }
    }
    return newPLRN;
  }

  /**
   *    RCAWE - Retrieve catch weight
   */
  def double RCAWE(DBContainer recMITALO) {
    String PXWHLO = recMITALO.get("MQWHLO");
    String PXITNO = recMITALO.get("MQITNO");
    String PXBANO = recMITALO.get("MQBANO");
    double PXTRQT = recMITALO.getDouble("MQTRQT");
    double SSTRQT = PXTRQT;
    int PXTTYP = recMITALO.getInt("MQTTYP");
    double PXCAWE = 0d;
    String PXERCD = ' ';
    String PXCNTW = ' ';

    SSTRQT = PXTRQT;
    if (PXTRQT < 0d) {
      PXTRQT = -(PXTRQT);
    }
    String PPUN = "";
    String UNMS = "";
    String SPUN = "";
    String CWUN = "";
    int CAWP = 0;
    int ACTI = 0;
    int DCCD = 0;
    DBAction queryMITMAS = database.table("MITMAS")
      .index("00")
      .selection("MMPPUN", "MMUNMS", "MMSPUN", "MMACTI", "MMDCCD", "MMCWUN")
      .build();
    DBContainer containerMITMAS = queryMITMAS.getContainer();
    containerMITMAS.set("MMCONO", XXCONO.toInteger());
    containerMITMAS.set("MMITNO", recMITALO.getString("ITNO").trim());
    if (queryMITMAS.read(containerMITMAS)) {
      PPUN = containerMITMAS.getString("MMPPUN").trim();
      UNMS = containerMITMAS.getString("MMUNMS").trim();
      SPUN = containerMITMAS.getString("MMSPUN").trim();
      CWUN = containerMITMAS.getString("MMCWUN").trim();
      CAWP = containerMITMAS.getInt("MMCAWP");
      ACTI = containerMITMAS.getInt("MMACTI");
      DCCD = containerMITMAS.getInt("MMDCCD");
    }
    if (PPUN == "") {
      PPUN = UNMS;
    }
    if (ACTI == 4) {
      PPUN = SPUN;
    }
    if (PXCAWE == 0) {
      if (PXTTYP == 11 && PXBANO != "" ||
        PXTTYP == 10 && PXBANO != "" && SSTRQT < 0d ||
        PXTTYP == 90 && PXBANO != "" ||
        PXTTYP == 31 && PXBANO != "" ||
        PXTTYP == 41 && PXBANO != "" ||
        PXTTYP == 51 && PXBANO != "" ||
        PXTTYP == 92 && PXBANO != "" ||
        PXTTYP == 93 && PXBANO != "" ||
        PXCNTW == '1' ||
        PXTTYP == 50 && PXBANO != "" ||
        PXTTYP == 97 && PXBANO != "" ||
        PXTTYP == 98 && PXBANO != "") {
        // ****************************************************************
        //   Calculate catch weight
        //   Withdrawal
        //   Calculate from MILOMC
        PXCAWE = CLCILO(recMITALO, PPUN, CWUN, UNMS, CAWP, DCCD, PXTRQT);
      }
      //   Indelivery
      if (PXCAWE == 0) {
        if (PXTTYP == 10 ||
          PXTTYP == 17 ||
          PXTTYP == 12 ||
          PXTTYP == 18 ||
          PXTTYP == 13 ||
          PXTTYP == 19 ||
          PXTTYP == 11 && PXBANO == "" ||
          PXTTYP == 31 && PXBANO == "" ||
          PXTTYP == 90 && PXCAWE == 0 ||
          PXTTYP == 25 ||
          PXTTYP == 40 ||
          PXTTYP == 50 ||
          PXTTYP == 41 && PXBANO == "" ||
          PXTTYP == 51 && PXBANO == "" ||
          PXTTYP == 92 && PXBANO == "" ||
          PXTTYP == 93 && PXBANO == "" ||
          PXTTYP == 31 && PXTRQT > 0 ||
          PXTTYP == 97 && PXTRQT > 0 ||
          PXTTYP == 98 && PXTRQT > 0) {
          //   Calculate from MITAUN
          PXCAWE = CLCAUN(recMITALO, PPUN, CWUN, UNMS, CAWP, DCCD, PXTRQT);
        }
      }
    }
    return PXCAWE;
  }

  /**
   *    CLCAUN - Calculate catch weight from MITAUN
   */
  def double CLCAUN(DBContainer recMITALO, String PPUN, String CWUN, String UNMS, int CAWP, int DCCD, double PXTRQT) {
    double PXCAWE = PXTRQT;
    int MUDCCD = DCCD;
    double MUCOFA = 1d;
    int MUDMCF = 0;
    double PXNUM = PXTRQT;
    String PXITNO = recMITALO.getString("MQITNO").trim();

    DBAction queryMITAUN = database.table("MITAUN")
      .index("00")
      .selection("MUDCCD", "MUDMCF", "MUCOFA")
      .build();
    DBContainer containerMITAUN = queryMITAUN.getContainer();
    containerMITAUN.set("MUCONO", XXCONO.toInteger());
    containerMITAUN.set("MUITNO", PXITNO);
    if (CAWP == 0) {
      if (PPUN != UNMS) {
        containerMITAUN.set("MUAUTP", 2);
        containerMITAUN.set("MUALUN", PPUN);
      }
    }
    if (CAWP == 1) {
      if (CWUN != UNMS) {
        containerMITAUN.set("MUAUTP", 2);
        containerMITAUN.set("MUALUN", CWUN);
      }
    }
    if (queryMITAUN.read(containerMITAUN)) {
      MUDCCD = containerMITAUN.getInt("MUDCCD");
      MUDMCF = containerMITAUN.getInt("MUDMCF");
      MUCOFA = containerMITAUN.getDouble("MUCOFA");
      //   Convert qty into catch weight unit
      if (MUDMCF == 1) {
        PXNUM /= MUCOFA;
      } else {
        PXNUM *= MUCOFA;
      }
      //  Fix decimal adjustment
      BigDecimal bigDecimal = new BigDecimal(PXNUM.toString());
      int scale = MUDCCD;
      PXCAWE = bigDecimal.setScale(scale, RoundingMode.CEILING);
    }
    return PXCAWE;
  }

  /**
   *    CLCILO - Calculate catch weight from MILOMC
   */
  def double CLCILO(DBContainer recMITALO, String PPUN, String CWUN, String UNMS, int CAWP, int DCCD, double PXTRQT) {
    double PXCAWE = 0d;
    double XSLASK = 0d;
    double XSLAS1 = 0d;
    double XSLAS2 = 0d;
    XSLASK = PXTRQT;

    String PXWHLO = recMITALO.get("MQWHLO");
    String PXITNO = recMITALO.get("MQITNO");
    String PXBANO = recMITALO.get("MQBANO");

    double IQTY = 0;
    double ACUQ = 0;
    double INCW = 0;
    double ACUW = 0;
    DBAction queryMILOMC = database.table("MILOMC")
      .index("00")
      .selection("LCIQTY", "LCACUQ", "LCINCW", "LCACUW")
      .build();
    DBContainer containerMILOMC = queryMILOMC.getContainer();
    containerMILOMC.set("LCCONO", XXCONO.toInteger());
    containerMILOMC.set("LCWHLO", PXWHLO);
    containerMILOMC.set("LCITNO", PXITNO);
    containerMILOMC.set("LCBANO", PXBANO);
    if (queryMILOMC.read(containerMILOMC)) {
      IQTY = containerMILOMC.getDouble("LCIQTY");
      ACUQ = containerMILOMC.getDouble("LCACUQ");
      INCW = containerMILOMC.getDouble("LCINCW");
      ACUW = containerMILOMC.getDouble("LCACUW");
      XSLASK = IQTY - ACUQ;
      XSLAS1 = INCW - ACUW;
      if (XSLASK > 0 && XSLAS1 > 0) {
        XSLAS2 = PXTRQT;
        if (XSLASK != XSLAS2) {
          XSLASK = XSLAS1/XSLASK;
          XSLASK *= PXTRQT;
        } else {
          XSLASK = XSLAS1;
        }
      } else {
        XSLASK = XSLAS1;
      }
      int MUDCCD = DCCD;
      DBAction queryMITAUN = database.table("MITAUN")
        .index("00")
        .selection("MUDCCD")
        .build();
      DBContainer containerMITAUN = queryMITAUN.getContainer();
      containerMITAUN.set("MUCONO", XXCONO.toInteger());
      containerMITAUN.set("MUITNO", PXITNO);
      if (CAWP == 0) {
        if (PPUN != UNMS) {
          containerMITAUN.set("MUAUTP", 2);
          containerMITAUN.set("MUALUN", PPUN);
        }
      }
      if (CAWP == 1) {
        if (CWUN != UNMS) {
          containerMITAUN.set("MUAUTP", 2);
          containerMITAUN.set("MUALUN", CWUN);
        }
      }
      if (queryMITAUN.read(containerMITAUN)) {
        MUDCCD = containerMITAUN.getInt("MUDCCD");
        //  Fix decimal adjustment
        BigDecimal bigDecimal = new BigDecimal(XSLASK.toString());
        int scale = MUDCCD;
        PXCAWE = bigDecimal.setScale(scale, RoundingMode.CEILING);
      }
    }
    return PXCAWE;
  }

  /**
   * getItemDtls - read MITMAS
   * @param ITNO
   */
  def void getItemDtls(String ITNO) {
    MMCAWP = 0;
    MMGRWE = 0D;
    MMVOL3 = 0D;
    MMGRTS = "";
    DBAction query = database.table("MITMAS").index("00")
      .selection("MMCAWP", "MMGRWE", "MMVOL3", "MMGRTS")
      .build();
    DBContainer container = query.getContainer();
    container.set("MMCONO", XXCONO.toInteger());
    container.set("MMITNO", ITNO);
    if (query.read(container)) {
      MMCAWP = container.getInt("MMCAWP");
      MMGRWE = container.getDouble("MMGRWE");
      MMVOL3 = container.getDouble("MMVOL3");
      MMGRTS = container.getString("MMGRTS").trim();
    }
  }

  /**
   * processMFTRND - Change MITALO references by deleting the MFTRND record with reporting nunber from the current MITALO record
   * @param recMFTRND
   * @param savePLRN
   * @param newPLRN
   * @return
   */
  def processMFTRND(DBContainer recMFTRND, long savePLRN, long newPLRN) {
    // Change MITALO references by deleting the MFTRND record with reporting nunber from the current MITALO record
    Closure<?> deleteMFTRND = { LockedResult lockedResult ->
      lockedResult.delete();
    }
    DBAction queryMFTRND = database.table("MFTRND")
      .index("00")
      .build();
    DBContainer containerMFTRND = queryMFTRND.getContainer();
    containerMFTRND.set("O0CONO", recMFTRND.getInt("O0CONO"));
    containerMFTRND.set("O0WHLO", recMFTRND.getString("O0WHLO").trim());
    containerMFTRND.set("O0DLIX", Long.parseLong(recMFTRND.get("O0DLIX").toString().trim()));
    containerMFTRND.set("O0PANR", recMFTRND.getString("O0PANR").trim());
    containerMFTRND.set("O0RORC", recMFTRND.getInt("O0RORC"));
    containerMFTRND.set("O0RIDN", recMFTRND.getString("O0RIDN").trim());
    containerMFTRND.set("O0RIDL", recMFTRND.getInt("O0RIDL"));
    containerMFTRND.set("O0RIDX", recMFTRND.getInt("O0RIDX"));
    containerMFTRND.set("O0BANO", recMFTRND.getString("O0BANO").trim());
    containerMFTRND.set("O0CAMU", recMFTRND.getString("O0CAMU").trim());
    containerMFTRND.set("O0PLRN", savePLRN);
    queryMFTRND.readLock(containerMFTRND, deleteMFTRND);
    // and create a new MFTRND with reporting number from the the found/created MITALO record
    containerMFTRND.set("O0PLRN", newPLRN);
    queryMFTRND.insert(containerMFTRND);
  }

  /**
   * delAllocationLine
   * @param recMITALO - delete allocation line
   * @return
   */
  def delAllocationLine(DBContainer recMITALO) {
    Closure<?> deleteMITALO = { LockedResult lockedResult ->
      // Delet allocation line
      lockedResult.delete();
    }
    DBAction queryDelMITALO = database.table("MITALO")
      .index("00")
      .selection("MQTRQT", "MQALQT", "MQPAQT")
      .build();
    DBContainer container = queryDelMITALO.getContainer()
    container.set("MQCONO", recMITALO.getInt("MQCONO"));
    container.set("MQWHLO", recMITALO.getString("MQWHLO").trim());
    container.set("MQITNO", recMITALO.getString("MQITNO").trim());
    container.set("MQWHSL", recMITALO.getString("MQWHSL").trim());
    container.set("MQBANO", recMITALO.getString("MQBANO").trim());
    container.set("MQCAMU", recMITALO.getString("MQCAMU").trim());
    container.set("MQTTYP", recMITALO.getInt("MQTTYP"));
    container.set("MQRIDN", recMITALO.getString("MQRIDN").trim());
    container.set("MQRIDO", recMITALO.getInt("MQRIDO"));
    container.set("MQRIDL", recMITALO.getInt("MQRIDL"));
    container.set("MQRIDX", recMITALO.getInt("MQRIDX"));
    container.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    container.set("MQPLSX", recMITALO.getInt("MQPLSX"));
    container.set("MQSOFT", recMITALO.getInt("MQSOFT"));
    if (queryDelMITALO.read(container)) {
      if (container.getDouble("MQTRQT") <= 0
        && container.getDouble("MQALQT") <= 0
        && container.getDouble("MQPAQT") <= 0) {
        queryDelMITALO.readLock(container, deleteMITALO);
      }
    }
  }

  /**
   * delMHPICH - delete MHPICH
   * @param recMHPICH
   * @param orgITALOPLSX
   * @return
   */
  def delMHPICH(DBContainer recMHPICH, int orgITALOPLSX) {
    Closure<?> deleteMHPICH = { LockedResult lockedResult ->
      lockedResult.delete();
    }
    DBAction queryMHPICH = database.table("MHPICH")
      .index("00")
      .build();
    DBContainer containerMHPICH = queryMHPICH.getContainer();
    containerMHPICH.set("PICONO", XXCONO.toInteger());
    containerMHPICH.setLong("PIDLIX", recMHPICH.getLong("PIDLIX"));
    containerMHPICH.set("PIPLSX", orgITALOPLSX);
    queryMHPICH.readLock(containerMHPICH, deleteMHPICH);
  }

  /**
   * updLastMHPICH - upddate Last MHPICH
   * @param recMHPICH
   * @return
   */
  def updLastMHPICH(DBContainer recMHPICH) {
    Closure<?> updateMHPICH = { LockedResult lockedResult ->
      lockedResult.set("PIMXPP", MCMXPP);
      lockedResult.set("PIMXPW", MCMXPW);
      lockedResult.set("PIMXPV", MCMXPV);
      lockedResult.set("PIMXPT", MCMXPT);
      lockedResult.set("PIMXPL", MCMXPL);
      if (checkNoOfPackages) {
        lockedResult.set("PICLPP", PAKCNT);
      } else {
        lockedResult.set("PICLPP", 0);
      }
      if (checkItemWeight) {
        lockedResult.set("PICLPW", PKGRWE);
      } else {
        lockedResult.set("PICLPW", 0d);
      }
      if (checkItemVolume) {
        lockedResult.set("PICLPV", PKVOL3);
      } else {
        lockedResult.set("PICLPV", 0d);
      }
      if (checkPickTime) {
        lockedResult.set("PICLPT", TIMCNT);
      } else {
        lockedResult.set("PICLPT", 0d);
      }
      if (checkPickLines) {
        lockedResult.set("PICLPL", LINCNT);
      } else {
        lockedResult.set("PICLPL", 0);
      }
      lockedResult.set("PILMDT", currentDate);
      lockedResult.set("PICHID", program.getUser());
      lockedResult.set("PICHNO", lockedResult.getInt("PICHNO") + 1);
      lockedResult.update();
      // Print pick list
      if (EDPRTA == 1 && EDPIRL == 2) {
        PRPIC(lockedResult);
      }
    }
    DBAction queryMHPICH = database.table("MHPICH")
      .index("00")
      .build();
    DBContainer containerMHPICH = queryMHPICH.getContainer()
    containerMHPICH.set("PICONO", XXCONO.toInteger());
    containerMHPICH.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
    containerMHPICH.set("PIPLSX", recMHPICH.getInt("PIPLSX"));
    queryMHPICH.readLock(containerMHPICH, updateMHPICH);
  }

  /**
   *    PRPIC - Perform PickingList
   */
  def void PRPIC(DBContainer recMHPICH) {
    String xrand = getRandomNum();

    DBAction query = database.table("MITDPR")
      .index("00")
      .selection("MPBJNO")
      .build();
    DBContainer container = query.getContainer()
    container.set("MPBJNO", xrand)
    container.set("MPCONO", XXCONO.toInteger());
    container.setLong("MPRIDI", recMHPICH.getLong("PIDLIX"));
    container.set("MPPLSX", recMHPICH.getInt("PIPLSX"));
    container.set("MPSLTP", recMHPICH.getString("PISLTP"));
    container.set("MPPISE", recMHPICH.getString("PIPISE"));
    container.set("MPPLRI", recMHPICH.getString("PIPLRI"));
    container.set("MPWHLO", recMHPICH.getString("PIWHLO"));
    container.set("MPUSID", program.getUser());
    container.set("MPDOVA", EDDOVA);
    String MPPGNM = "";
    if (EDDOVA == "10") {
      MPPGNM = "MWS438";
      container.set("MPPGNM", MPPGNM);
    } else if (EDDOVA == "20") {
      MPPGNM = "MWS436";
      container.set("MPPGNM", MPPGNM);
    } else {
      MPPGNM = "MWS435";
      container.set("MPPGNM", MPPGNM);
    }
    if (query.insert(container)) {
      R972(xrand, recMHPICH.getString("PIWHLO"), MPPGNM);
    }
  }

  /**
   * R972 - write MMW972
   * @param xrand
   * @param whlo
   */
  def void R972(String xrand, String whlo, String MPPGNM) {
    DBAction ActionMW972 = database.table("MMW972").build();
    DBContainer MW972 = ActionMW972.getContainer();
    MW972.set("J2CONO", XXCONO.toInteger());
    MW972.set("J2WHLO", whlo);
    MW972.set("J2BJNO", xrand);
    MW972.set("J2PGNM", MPPGNM);
    MW972.set("J2RGDT", currentDate);
    MW972.set("J2RGTM", currentTime);
    MW972.set("J2CHID", program.getUser());
    ActionMW972.insert(MW972);
    picklistPrintJob = true;
  }

  /**
   * getRandomNum - generate random number
   * @return
   */
  def String getRandomNum() {
    Random rnd = new Random();
    String xrand = "";
    int number = 0;
    for(int i=0; i<18; i++) {
      number = rnd.nextInt(9);
      xrand += String.format("%01d", number);
    }
    return xrand;
  }

  /**
   *    CPSPLT  - Calculate package based split
   */
  public void CPSPLT(DBContainer recMHPICH) {
    // Level breaks
    breakOnLine = false;
    breakOnPackage = false;
    breakOnTime = false;
    breakOnWeight = false;
    breakOnVolume = false;
    updatePackInfo = false;
    lineUpdate = false;
    // Work flow
    splitPerformed = false;

    // Allocations
    int XSPLSX = recMHPICH.getInt("PIPLSX");
    List<DBContainer> lstMFTRND = readMFTRND00ByMHPICH(recMHPICH);
    for (DBContainer recMFTRND : lstMFTRND) {
      def queryMITALO = database.table("MITALO")
        .index("50")
        .selectAllFields()
        .build();
      def containerMITALO = queryMITALO.createContainer();
      containerMITALO.set("MQCONO", XXCONO.toInteger());
      containerMITALO.setLong("MQPLRN", recMFTRND.getLong("O0PLRN"));
      containerMITALO.set("MQPLSX", recMHPICH.getInt("PIPLSX"));
      queryMITALO.readAll(containerMITALO, 2, { DBContainer record ->
        if (containerMITALO.getInt("MQPLSX") == XSPLSX) {
          // Split on single package - create next suffix and reset counts
          if (breakOnPackage) {
            UPLSX(recMHPICH);
            breakOnPackage = false;
            LINCNT = 0;
            PAKCNT = 0;
            LINWCT = 0;
            PKGRWE = 0D;
            PKVOL3 = 0D;
            TIMCNT = 0D;
            TIMWCT = 0D;
            PKDLQT = 0D;
          }
          // Adjust reporting number on MFTRND
          updatePackInfo = false;
          LINWCT++;
          LINCNT++;
          PKDLQT = 0D;
          // Perform line break check
          if (checkPickLines &&
            LINCNT > MCMXPL) {
            XXPCSI = 5;
            breakOnLine = true;
            lineUpdate = true;
            RSPLIT(recMHPICH, record);
            breakOnLine = false;
          } else {
            // Break already hit - update all subsequent lines
            if (lineUpdate) {
              updSubLines(record, recMHPICH);
            }
          }
          // Perform accumulations
          List<DBContainer> lstMFTRND10 = readMFTRND10ByMITALO(record);
          for (DBContainer recMFTRND10 : lstMFTRND10) {
            // Split on single package
            if (breakOnPackage) {
              updatePackInfo = true;
              lineUpdate = true;
              RSPLIT(recMHPICH, record);
              breakOnPackage = false;
            }
            double O0DLQT = recMFTRND10.getDouble("O0DLQT");
            PKDLQT += O0DLQT;
            //   Retrieve item
            getItemDtls(record.getString("MQITNO").trim());
            if (EDCPTM == 1) {
              CPTM(record, O0DLQT);
              TIMCNT += XXNUM;
              TIMWCT += XXNUM;
            }
            XXGRWE = MMGRWE * O0DLQT;
            XXVOL3 = MMVOL3 * O0DLQT;
            PKGRWE += XXGRWE;
            PKVOL3 += XXVOL3;
            // Perform checks (at lowest level)
            // Weight
            if (checkItemWeight) {
              if (PKGRWE > MCMXPW) {
                XXPCSI = 2;
                updatePackInfo = true;
                lineUpdate = true;
                breakOnWeight = true;
                // Need to check if breaking on a single package
                if (PAKCNT == 1) {
                  breakOnPackage = true;
                } else {
                  // Remove latest totals and update retrospectively
                  PKDLQT -= O0DLQT;
                  TIMCNT -= XXNUM;
                  PKGRWE -= XXGRWE;
                  PKVOL3 -= XXVOL3;
                  PAKCNT--;
                  // Create next suffix and prime next MITALO record
                  RSPLIT(recMHPICH, record);
                  updMFTRND_PLRN(recMFTRND10, record.getLong("MQPLRN"));
                  // Restore current totals
                  PKDLQT += O0DLQT;
                  TIMCNT += XXNUM;
                  PKGRWE += XXGRWE;
                  PKVOL3 += XXVOL3;
                }
              }
            }
            // Volume
            if (checkItemVolume && !breakOnWeight) {
              if (PKVOL3 > MCMXPV) {
                XXPCSI = 3;
                updatePackInfo = true;
                lineUpdate = true;
                breakOnVolume = true;
                // Need to check if breaking on a single package
                if (PAKCNT == 1) {
                  breakOnPackage = true;
                } else {
                  // Remove latest totals and update retrospectively
                  PKDLQT -= O0DLQT;
                  TIMCNT -= XXNUM;
                  PKGRWE -= XXGRWE;
                  PKVOL3 -= XXVOL3;
                  PAKCNT--;
                  // Create next suffix and prime next MITALO record
                  RSPLIT(recMHPICH, record);
                  updMFTRND_PLRN(recMFTRND10, record.getLong("MQPLRN"));
                  // Restore current totals
                  PKDLQT += O0DLQT;
                  TIMCNT += XXNUM;
                  PKGRWE += XXGRWE;
                  PKVOL3 += XXVOL3;
                }
              }
            }
            // Pick time
            if (checkPickTime &&
              !breakOnWeight &&
              !breakOnVolume &&
              EDCPTM == 1) {
              if (TIMCNT > MCMXPT) {
                XXPCSI = 4;
                updatePackInfo = true;
                lineUpdate = true;
                breakOnTime = true;
                // Need to check if breaking on a single package
                if (PAKCNT == 1) {
                  breakOnPackage = true;
                } else {
                  // Remove latest totals and update retrospectively
                  PKDLQT -= O0DLQT;
                  TIMCNT -= XXNUM;
                  PKGRWE -= XXGRWE;
                  PKVOL3 -= XXVOL3;
                  PAKCNT--;
                  // Create next suffix and prime next MITALO record
                  RSPLIT(recMHPICH, record);
                  updMFTRND_PLRN(recMFTRND10, record.getLong("MQPLRN"));
                  // Restore current totals
                  PKDLQT += O0DLQT;
                  TIMCNT += XXNUM;
                  PKGRWE += XXGRWE;
                  PKVOL3 += XXVOL3;
                }
              }
            }
            breakOnWeight = false;
            breakOnVolume = false;
            breakOnTime = false;
          }
        }
        // Outstanding quantity - split was at package level adjust current record
        if (PKDLQT != 0 &&
          updatePackInfo ||
          breakOnPackage) {
          updOutstandingQty(record);
        }
      });
    }
    DBAction query = database.table("MHPICH")
      .index("00")
      .reverse()
      .selectAllFields()
      .build();
    DBContainer container = query.getContainer();
    container.set("PICONO", XXCONO.toInteger());
    container.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
    boolean isFirstRecord = true;
    query.readAll(container, 2, 1,{ DBContainer record ->
      if (isFirstRecord) {
        recMHPICH = record;
        isFirstRecord = false;
      }
    });
    // Update last MHPICH
    if (splitPerformed || lineUpdate && LINCNT > 0) {
      Closure<?> updateMHPICH = { LockedResult lockedResult ->
        lockedResult.set("PINPLL", LINCNT);
        lockedResult.set("PIPLTM", TIMCNT);
        lockedResult.update();
      }
      DBAction queryMHPICH = database.table("MHPICH")
        .index("00")
        .build();
      DBContainer containerMHPICH = queryMHPICH.getContainer();
      containerMHPICH.set("PICONO", XXCONO.toInteger());
      containerMHPICH.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
      containerMHPICH.set("PIPLSX", recMHPICH.getInt("PIPLSX"));
      queryMHPICH.readLock(containerMHPICH, updateMHPICH);
    }
    // Update last MHPICH - execute even if no split performed
    updLastMHPICH(recMHPICH);
  }

  /**
   * readMFTRND00ByMHPICH - read MFTRND
   * @param recMHPICH
   * @return
   */
  def List<DBContainer> readMFTRND00ByMHPICH(DBContainer recMHPICH) {
    List<DBContainer> containers = new ArrayList();
    def queryMFTRND = database.table("MFTRND")
      .index("00")
      .selection("O0DLIX", "O0PANR", "O0DLQT", "O0PLRN")
      .build();
    def containerMFTRND = queryMFTRND.createContainer();
    containerMFTRND.set("O0CONO", XXCONO.toInteger());
    containerMFTRND.set("O0WHLO", recMHPICH.getString("PIWHLO"));
    containerMFTRND.set("O0DLIX", Long.parseLong(recMHPICH.get("PIDLIX").toString().trim()));
    queryMFTRND.readAll(containerMFTRND, 3, { DBContainer recordMFTRND ->
      containers.add(recordMFTRND.createCopy());
    });
    return containers;
  }

  /**
   * updSubLines - update all subsequent lines
   * @param recMITALO
   * @param recMHPICH
   * @return
   */
  def updSubLines(DBContainer recMITALO, DBContainer recMHPICH) {
    Closure<?> updateMITALO = { LockedResult lockedResult ->
      lockedResult.set("MQPLSX", recMHPICH.getInt("PIPLSX"));
      lockedResult.set("MQLMDT", currentDate);
      lockedResult.set("MQCHID", program.getUser());
      lockedResult.set("MQCHNO", lockedResult.getInt("MQCHNO") + 1);
      lockedResult.update();
    }
    DBAction queryMITALO = database.table("MITALO")
      .index("00")
      .build();
    DBContainer containerMITALO = queryMITALO.getContainer();
    containerMITALO.set("MQCONO", XXCONO.toInteger())
    containerMITALO.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMITALO.set("MQPLSX", recMITALO.getInt("MQPLSX"));
    containerMITALO.set("MQCAMU", recMITALO.getString("MQCAMU"));
    containerMITALO.set("MQITNO", recMITALO.getString("MQITNO"));
    containerMITALO.set("MQWHLO", recMITALO.getString("MQWHLO"));
    containerMITALO.set("MQWHSL", recMITALO.getString("MQWHSL"));
    containerMITALO.set("MQBANO", recMITALO.getString("MQBANO"));
    containerMITALO.set("MQTTYP", recMITALO.getInt("MQTTYP"));
    containerMITALO.set("MQRIDN", recMITALO.getString("MQRIDN"));
    containerMITALO.set("MQRIDO", recMITALO.getInt("MQRIDO"));
    containerMITALO.set("MQRIDL", recMITALO.getInt("MQRIDL"));
    containerMITALO.set("MQRIDX", recMITALO.getInt("MQRIDX"));
    containerMITALO.set("MQSOFT", recMITALO.getInt("MQSOFT"));
    queryMITALO.readLock(containerMITALO, updateMITALO);
  }

  /**
   * readMFTRND10ByMITALO - read records for MFTRND
   * @param recMITALO
   * @return
   */
  def List<DBContainer> readMFTRND10ByMITALO(DBContainer recMITALO) {
    List<DBContainer> containers = new ArrayList();
    def queryMFTRND = database.table("MFTRND")
      .index("10")
      .selection("O0DLIX", "O0PANR", "O0DLQT")
      .build();
    def containerMFTRND = queryMFTRND.createContainer();
    containerMFTRND.set("O0CONO", XXCONO.toInteger());
    containerMFTRND.set("O0WHLO", recMITALO.getString("MQWHLO"));
    containerMFTRND.set("O0DLIX", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMFTRND.set("O0RORC", recMITALO.getInt("MQTTYP") / 10);
    containerMFTRND.set("O0RIDN", recMITALO.getString("MQRIDN"));
    containerMFTRND.set("O0RIDL", recMITALO.getInt("MQRIDL"));
    containerMFTRND.set("O0RIDX", recMITALO.getInt("MQRIDX"));
    containerMFTRND.set("O0PLRN", Long.parseLong(recMITALO.get("MQPLRN").toString().trim()));
    containerMFTRND.set("O0BANO", recMITALO.getString("MQBANO"));
    containerMFTRND.set("O0CAMU", recMITALO.getString("MQCAMU"));
    queryMFTRND.readAll(containerMFTRND, 10, { DBContainer recordMFTRND ->
      containers.add(recordMFTRND.createCopy());
    });
    return containers;
  }

  /**
   * updMFTRND_PLRN - update MFTRND
   * @param recMFTRND
   * @param PLRN
   * @return
   */
  def updMFTRND_PLRN(DBContainer recMFTRND, long PLRN) {
    Closure<?> updMFTRND = { LockedResult lockedResult ->
      lockedResult.set("O0PLRN", PLRN);
      lockedResult.update();
    }
    DBAction queryMFTRND = database.table("MFTRND")
      .index("00")
      .build();
    DBContainer containerMFTRND = queryMFTRND.getContainer();
    containerMFTRND.set("O0CONO", recMFTRND.getInt("O0CONO"));
    containerMFTRND.set("O0WHLO", recMFTRND.getString("O0WHLO").trim());
    containerMFTRND.set("O0DLIX", Long.parseLong(recMFTRND.get("O0DLIX").toString().trim()));
    containerMFTRND.set("O0PANR", recMFTRND.getString("O0PANR").trim());
    containerMFTRND.set("O0RORC", recMFTRND.getInt("O0RORC"));
    containerMFTRND.set("O0RIDN", recMFTRND.getString("O0RIDN").trim());
    containerMFTRND.set("O0RIDL", recMFTRND.getInt("O0RIDL"));
    containerMFTRND.set("O0RIDX", recMFTRND.getInt("O0RIDX"));
    containerMFTRND.set("O0BANO", recMFTRND.getString("O0BANO").trim());
    containerMFTRND.set("O0CAMU", recMFTRND.getString("O0CAMU").trim());
    containerMFTRND.set("O0PLRN", recMFTRND.getLong("O0PLRN"));
    queryMFTRND.readLock(containerMFTRND, updMFTRND);
  }

  /**
   * updOutstandingQty - Outstanding quantity - split was at package level adjust current record
   * @param recMITALO
   * @return
   */
  def updOutstandingQty(DBContainer recMITALO) {
    Closure<?> updateMITALO = { LockedResult lockedResult ->
      lockedResult.set("MQTRQT", PKDLQT);
      lockedResult.set("MQALQT", PKDLQT);
      lockedResult.set("MQPAQT", PKDLQT);
      lockedResult.set("MQALQN", 0D);
      if (lockedResult.getDouble("MQCAWE") != 0 && MMCAWP == 0) {
        lockedResult.set("MQCAWE", RCAWE(lockedResult));
      }
      // Adjust existing allocation line
      lockedResult.set("MQLMDT", currentDate);
      lockedResult.set("MQCHID", program.getUser());
      lockedResult.set("MQCHNO", lockedResult.getInt("MQCHNO") + 1);
      lockedResult.update();
      lineUpdate = true;
    }
    DBAction queryMITALO = database.table("MITALO")
      .index("00")
      .build();
    DBContainer containerMITALO = queryMITALO.getContainer();
    containerMITALO.set("MQCONO", XXCONO.toInteger())
    containerMITALO.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMITALO.set("MQPLSX", recMITALO.getInt("MQPLSX"));
    containerMITALO.set("MQCAMU", recMITALO.getString("MQCAMU"));
    containerMITALO.set("MQITNO", recMITALO.getString("MQITNO"));
    containerMITALO.set("MQWHLO", recMITALO.getString("MQWHLO"));
    containerMITALO.set("MQWHSL", recMITALO.getString("MQWHSL"));
    containerMITALO.set("MQBANO", recMITALO.getString("MQBANO"));
    containerMITALO.set("MQTTYP", recMITALO.getInt("MQTTYP"));
    containerMITALO.set("MQRIDN", recMITALO.getString("MQRIDN"));
    containerMITALO.set("MQRIDO", recMITALO.getInt("MQRIDO"));
    containerMITALO.set("MQRIDL", recMITALO.getInt("MQRIDL"));
    containerMITALO.set("MQRIDX", recMITALO.getInt("MQRIDX"));
    containerMITALO.set("MQSOFT", recMITALO.getInt("MQSOFT"));
    queryMITALO.readLock(containerMITALO, updateMITALO);
  }

  /**
   *    RSPLIT   - Split line MHPICH MITALO
   */
  def void RSPLIT(DBContainer recMHPICH, DBContainer recMITALO) {
    logger.debug("RSPLIT: packageCheck=${packageCheck}/breakOnLine=${breakOnLine}/fromspltNoOfPac=${fromspltNoOfPac}")
    // Next suffix
    UPLSX(recMHPICH);
    // Allocation adjustment
    if (packageCheck || breakOnLine) {
      if (!fromspltNoOfPac) {
        boolean isFirstRecord = true;
        Closure<?> getRecord = { DBContainer record ->
          if (isFirstRecord) {
            logger.debug("RSPLIT=${record}");
            UALO(recMITALO, record);
            isFirstRecord = false;
          }
        }
        DBAction query = database.table("MHPICH")
          .index("00")
          .reverse()
          .selectAllFields()
          .build();
        DBContainer container = query.getContainer();
        container.set("PICONO", XXCONO.toInteger());
        container.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
        query.readAll(container, 2, 1, getRecord);
      }
    }
    // Save variables
    if (!packageCheck && LINCNT == 1) {
      LINCNT = 0;
      LINWCT = 0;
    } else {
      LINCNT = 1;
      LINWCT = 1;
    }
    if (breakOnLine || breakOnPackage) {
      PAKCNT = 0;
    } else {
      PAKCNT = 1;
    }
    PKGRWE = 0D;
    PKVOL3 = 0D;
    TIMCNT = 0D;
    TIMWCT = 0D;
    PKDLQT = 0D;
    picklistPrintJob = false;
  }

  /**
   *    UPLSX  - Update Pickinglist status
   */
  def void UPLSX(DBContainer recMHPICH) {
    // None package based split. Only increment pick list suffix if further MITALO records to process
    logger.debug("UPLSX:packageCheck=${packageCheck}/TOTCNT=${TOTCNT}/XXNPLL=${XXNPLL}/LINCNT=${LINCNT}")
    if (packageCheck ||
      !packageCheck &&
      TOTCNT < XXNPLL ||
      !packageCheck &&
      TOTCNT == XXNPLL &&
      LINCNT > 1) {
      Closure<?> updMHDISH = { LockedResult lockedResult ->
        lockedResult.set("OQPLSX", lockedResult.getInt("OQPLSX") + 1);
        lockedResult.update()
      }
      DBAction query = database.table("MHDISH")
        .index("00")
        .build();
      DBContainer container = query.getContainer();
      container.set("OQCONO", XXCONO.toInteger());
      container.set("OQINOU", 1);
      container.set("OQDLIX", recMHPICH.getLong("PIDLIX"));
      query.readLock(container, updMHDISH);
    }
    // Create Picklist Header
    UPICH(recMHPICH);
  }

  /**
   *    UPICH  - Update Pickinglist Header
   */
  public void UPICH(DBContainer recMHPICH) {
    logger.debug("UPICH=${recMHPICH}");
    logger.debug("UPICH breakOnLine=${breakOnLine}/LINCNT=${LINCNT}/PKDLQT=${PKDLQT}/packageCheck=${packageCheck}/inTEAM=${inTEAM}/inPICK=${inPICK}")
    Closure<?> updMHPICH = { LockedResult lockedResult ->
      // Break level is at MITALO level lines equals line count minus current record
      if (breakOnLine &&
        LINCNT > 1 ||
        PKDLQT == 0 &&
        packageCheck ||
        !packageCheck &&
        LINCNT > 1) {
        lockedResult.set("PINPLL", (LINCNT - 1));
        if (checkPickLines) {
          lockedResult.set("PICLPL", (LINCNT - 1));
        } else {
          lockedResult.set("PICLPL", 0);
        }
      } else {
        lockedResult.set("PINPLL", LINCNT);
        if (checkPickLines) {
          lockedResult.set("PICLPL", LINCNT);
        } else {
          lockedResult.set("PICLPL", 0);
        }
      }
      lockedResult.set("PIPLTM", TIMCNT);
      if (!packageCheck) {
        lockedResult.set("PIMXPP", 0);
      } else {
        lockedResult.set("PIMXPP", MCMXPP);
      }
      lockedResult.set("PIMXPW", MCMXPW);
      lockedResult.set("PIMXPV", MCMXPV);
      lockedResult.set("PIMXPT", MCMXPT);
      lockedResult.set("PIMXPL", MCMXPL);
      if (checkNoOfPackages) {
        lockedResult.set("PICLPP", PAKCNT);
      } else {
        lockedResult.set("PICLPP", 0);
      }
      if (checkItemWeight) {
        lockedResult.set("PICLPW", PKGRWE);
      } else {
        lockedResult.set("PICLPW", 0d);
      }
      if (checkItemVolume) {
        lockedResult.set("PICLPV", PKVOL3);
      } else {
        lockedResult.set("PICLPV", 0d);
      }
      if (checkPickTime) {
        lockedResult.set("PICLPT", TIMCNT);
      } else {
        lockedResult.set("PICLPT", 0d);
      }
      lockedResult.set("PIPCSI", XXPCSI);
      lockedResult.update();
    }
    DBAction query = database.table("MHPICH")
      .index("00")
      .build();
    DBContainer container = query.getContainer();
    container.set("PICONO", XXCONO.toInteger());
    container.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
    container.set("PIPLSX", recMHPICH.getInt("PIPLSX"));
    query.readLock(container, updMHPICH);
    // Print pick list
    if (EDPRTA == 1) {
      PRPIC(recMHPICH);
    }
    DBAction queryMHDISH = database.table("MHDISH")
      .index("00")
      .selection("OQPLSX")
      .build();
    DBContainer containerMHDISH = queryMHDISH.getContainer();
    containerMHDISH.set("OQCONO", XXCONO.toInteger());
    containerMHDISH.set("OQINOU", 1);
    containerMHDISH.set("OQDLIX", recMHPICH.getLong("PIDLIX"));
    queryMHDISH.read(containerMHDISH);
    int OQPLSX = containerMHDISH.getInt("OQPLSX");
    logger.debug("UPICH:OQPLSX=${OQPLSX}/packageCheck=${packageCheck}")

    //Read again to retrieve current values
    DBAction queryMHPICH = database.table("MHPICH")
      .index("00")
      .selectAllFields()
      .build();
    recMHPICH = query.getContainer();
    recMHPICH.setInt("PICONO", XXCONO.toInteger());
    recMHPICH.setLong("PIDLIX", inDLIX.toLong());
    recMHPICH.setInt("PIPLSX", inPLSX.toInteger());
    queryMHPICH.read(recMHPICH);
    logger.debug("readagain recMHPICH=${recMHPICH}")
    if (packageCheck) {
      container.set("PICONO", XXCONO.toInteger());
      container.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
      container.set("PIPLSX", OQPLSX);
      container.set("PINPLL", 1);
      container.set("PIPLTM", 0D);
      container.set("PIPCSI", XXPCSI);
      container.set("PIWHLO", recMHPICH.getString("PIWHLO"));
      container.set("PIPISS", recMHPICH.getString("PIPISS"));
      container.set("PISLTP", recMHPICH.getString("PISLTP"));
      container.set("PIPISE", recMHPICH.getString("PIPISE"));
      container.set("PISEEQ", recMHPICH.getInt("PISEEQ"));
      container.set("PIESPD", recMHPICH.getInt("PIESPD"));
      container.set("PIESPT", recMHPICH.getInt("PIESPT"));
      container.set("PITEAM", inTEAM);
      container.set("PIPICK", inPICK);
      container.set("PIPLRI", "");
      container.set("PIWPIC", recMHPICH.getString("PIWPIC"));
      container.set("PIARLD", recMHPICH.getInt("PIARLD"));
      container.set("PIARLT", recMHPICH.getInt("PIARLT"));
      container.set("PIARLE", recMHPICH.getInt("PIARLE"));
      container.set("PIARLF", recMHPICH.getInt("PIARLF"));
      container.set("PINPLR", recMHPICH.getInt("PINPLR"));
      container.set("PINPLP", recMHPICH.getInt("PINPLP"));
      container.set("PICLOD", recMHPICH.getInt("PICLOD"));
      container.set("PICLOT", recMHPICH.getInt("PICLOT"));
      container.set("PIDEV", recMHPICH.getString("PIDEV"));
      container.set("PIDEVW", recMHPICH.getString("PIDEVW"));
      container.set("PITWLO", recMHPICH.getString("PITWLO"));
      container.set("PIOLIX", recMHPICH.getLong("PIOLIX"));
      container.set("PIRVNO", recMHPICH.getInt("PIRVNO"));
      container.set("PIRGDT", currentDate);
      container.set("PIRGTM", currentTime);
      container.set("PILMDT", currentDate);
      container.set("PICHNO", 1);
      container.set("PICHID", program.getUser());
      container.set("PIDOVA", recMHPICH.getString("PIDOVA"));
      container.set("PIPIRL", recMHPICH.getInt("PIPIRL"));
      container.set("PIMXPP", recMHPICH.getInt("PIMXPP"));
      container.set("PIMXPW", recMHPICH.getDouble("PIMXPW"));
      container.set("PIMXPV", recMHPICH.getDouble("PIMXPV"));
      container.set("PIMXPT", recMHPICH.getDouble("PIMXPT"));
      container.set("PIMXPL", recMHPICH.getInt("PIMXPL"));
      container.set("PICLPP", recMHPICH.getInt("PICLPP"));
      container.set("PICLPW", recMHPICH.getDouble("PICLPW"));
      container.set("PICLPV", PKVOL3);
      container.set("PICLPT", recMHPICH.getDouble("PICLPT"));
      container.set("PICLPL", recMHPICH.getInt("PICLPL"));
      container.set("PIPCSP", recMHPICH.getInt("PIPCSP"));
      query.insert(container);
    } else {
      // None package based split. Only prime next pick header record if further MITALO records to process
      logger.debug("UPICH nonepackage TOTCNT=${TOTCNT}/XXNPLL=${XXNPLL}/LINCNT=${LINCNT}")
      if (TOTCNT < XXNPLL ||
        TOTCNT == XXNPLL &&
        LINCNT > 1) {
        container.set("PICONO", XXCONO.toInteger());
        container.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
        container.set("PIPLSX", OQPLSX);
        container.set("PINPLL", 1);
        container.set("PIPLTM", 0D);
        container.set("PIPCSI", XXPCSI);

        container.set("PIWHLO", recMHPICH.getString("PIWHLO"));
        container.set("PIPISS", recMHPICH.getString("PIPISS"));
        container.set("PISLTP", recMHPICH.getString("PISLTP"));
        container.set("PIPISE", recMHPICH.getString("PIPISE"));
        container.set("PISEEQ", recMHPICH.getInt("PISEEQ"));
        container.set("PIESPD", recMHPICH.getInt("PIESPD"));
        container.set("PIESPT", recMHPICH.getInt("PIESPT"));
        container.set("PITEAM", inTEAM);
        container.set("PIPICK", inPICK);
        container.set("PIPLRI", "");
        container.set("PIWPIC", recMHPICH.getString("PIWPIC"));
        container.set("PIARLD", recMHPICH.getInt("PIARLD"));
        container.set("PIARLT", recMHPICH.getInt("PIARLT"));
        container.set("PIARLE", recMHPICH.getInt("PIARLE"));
        container.set("PIARLF", recMHPICH.getInt("PIARLF"));
        container.set("PINPLR", recMHPICH.getInt("PINPLR"));
        container.set("PINPLP", recMHPICH.getInt("PINPLP"));
        container.set("PICLOD", recMHPICH.getInt("PICLOD"));
        container.set("PICLOT", recMHPICH.getInt("PICLOT"));
        container.set("PIDEV", recMHPICH.getString("PIDEV"));
        container.set("PIDEVW", recMHPICH.getString("PIDEVW"));
        container.set("PITWLO", recMHPICH.getString("PITWLO"));
        container.set("PIOLIX", recMHPICH.getLong("PIOLIX"));
        container.set("PIRVNO", recMHPICH.getInt("PIRVNO"));
        container.set("PIRGDT", currentDate);
        container.set("PIRGTM", currentTime);
        container.set("PILMDT", currentDate);
        container.set("PICHNO", 1);
        container.set("PICHID", program.getUser());
        container.set("PIDOVA", recMHPICH.getString("PIDOVA"));
        container.set("PIPIRL", recMHPICH.getInt("PIPIRL"));
        container.set("PIMXPP", recMHPICH.getInt("PIMXPP"));
        container.set("PIMXPW", recMHPICH.getDouble("PIMXPW"));
        container.set("PIMXPV", recMHPICH.getDouble("PIMXPV"));
        container.set("PIMXPT", recMHPICH.getDouble("PIMXPT"));
        container.set("PIMXPL", recMHPICH.getInt("PIMXPL"));
        container.set("PICLPP", recMHPICH.getInt("PICLPP"));
        container.set("PICLPW", recMHPICH.getDouble("PICLPW"));
        container.set("PICLPV", PKVOL3);
        container.set("PICLPT", recMHPICH.getDouble("PICLPT"));
        container.set("PICLPL", recMHPICH.getInt("PICLPL"));
        container.set("PIPCSP", recMHPICH.getInt("PIPCSP"));
        query.insert(container);
      }
    }
    XXPCSI = 0;
  }

  /**
   *    UALO  - Update Allocations
   */
  def void UALO(DBContainer recMITALO, DBContainer recMHPICH) {
    Closure<?> updateMITALO = { LockedResult lockedResult ->
      // Break level is at MITALO level simply adjust pick list suffix
      if (breakOnLine || PKDLQT == 0) {

        DBAction queryMITALO = database.table("MITALO")
          .index("00")
          .build();
        DBContainer containerMITALO = queryMITALO.getContainer();
        containerMITALO.set("MQCONO", lockedResult.get("MQCONO"));
        containerMITALO.set("MQWHLO", lockedResult.get("MQWHLO"));
        containerMITALO.set("MQITNO", lockedResult.get("MQITNO"));
        containerMITALO.set("MQWHSL", lockedResult.get("MQWHSL"));
        containerMITALO.set("MQBANO", lockedResult.get("MQBANO"));
        containerMITALO.set("MQCAMU", lockedResult.get("MQCAMU"));
        containerMITALO.set("MQREPN", lockedResult.get("MQREPN"));
        containerMITALO.set("MQTTYP", lockedResult.get("MQTTYP"));
        containerMITALO.set("MQRIDN", lockedResult.get("MQRIDN"));
        containerMITALO.set("MQRIDO", lockedResult.get("MQRIDO"));
        containerMITALO.set("MQRIDI", lockedResult.get("MQRIDI"));
        if (LINCNT > 1) {
          containerMITALO.set("MQPLSX", recMHPICH.get("PIPLSX"));
        } else {
          containerMITALO.set("MQPLSX", lockedResult.get("MQPLSX"));
        }
        containerMITALO.set("MQRIDL", lockedResult.get("MQRIDL"));
        containerMITALO.set("MQSLTP", lockedResult.get("MQSLTP"));
        containerMITALO.set("MQPISE", lockedResult.get("MQPISE"));
        containerMITALO.set("MQALQT", lockedResult.get("MQALQT"));
        containerMITALO.set("MQSORT", lockedResult.get("MQSORT"));
        containerMITALO.set("MQTRFL", lockedResult.get("MQTRFL"));
        containerMITALO.set("MQMAAL", lockedResult.get("MQMAAL"));
        containerMITALO.set("MQRFTX", lockedResult.get("MQRFTX"));
        containerMITALO.set("MQPGNM", lockedResult.get("MQPGNM"));
        containerMITALO.set("MQPLPR", lockedResult.get("MQPLPR"));
        containerMITALO.set("MQPAQT", lockedResult.get("MQPAQT"));
        containerMITALO.set("MQCAWE", lockedResult.get("MQCAWE"));
        containerMITALO.set("MQFLOC", lockedResult.get("MQFLOC"));
        containerMITALO.set("MQDEV", lockedResult.get("MQDEV"));
        containerMITALO.set("MQSTCD", lockedResult.get("MQSTCD"));
        containerMITALO.set("MQSOFT", lockedResult.get("MQSOFT"));
        containerMITALO.set("MQTRQT", lockedResult.get("MQTRQT"));
        containerMITALO.set("MQTWSL", lockedResult.get("MQTWSL"));
        containerMITALO.set("MQOEND", lockedResult.get("MQOEND"));
        containerMITALO.set("MQPLRI", lockedResult.get("MQPLRI"));
        containerMITALO.set("MQTRQA", lockedResult.get("MQTRQA"));
        containerMITALO.set("MQALQA", lockedResult.get("MQALQA"));
        containerMITALO.set("MQALQN", lockedResult.get("MQALQN"));
        containerMITALO.set("MQPAQA", lockedResult.get("MQPAQA"));
        containerMITALO.set("MQCNNR", lockedResult.get("MQCNNR"));
        containerMITALO.set("MQLSQN", lockedResult.get("MQLSQN"));
        containerMITALO.set("MQSSEQ", lockedResult.get("MQSSEQ"));
        containerMITALO.set("MQPLRN", lockedResult.get("MQPLRN"));
        containerMITALO.set("MQRGDT", lockedResult.get("MQRGDT"));
        containerMITALO.set("MQRGTM", lockedResult.get("MQRGTM"));
        containerMITALO.set("MQLMDT", lockedResult.get("MQLMDT"));
        containerMITALO.set("MQCHNO", lockedResult.get("MQCHNO"));
        containerMITALO.set("MQCHID", lockedResult.get("MQCHID"));
        containerMITALO.set("MQRIDX", lockedResult.get("MQRIDX"));
        containerMITALO.set("MQAUCZ", lockedResult.get("MQAUCZ"));
        containerMITALO.set("MQORRN", lockedResult.get("MQORRN"));
        containerMITALO.set("MQSUME", lockedResult.get("MQSUME"));
        queryMITALO.insert(containerMITALO);

//        if (LINCNT > 1) {
//          lockedResult.set("MQPLSX", recMHPICH.getInt("PIPLSX"));
//        }
        lockedResult.delete();

        // Break level is at "quantity" level update current MITALO with accumulated totals and
        // prime next record
      } else {
        lockedResult.set("MQTRQT", PKDLQT);
        lockedResult.set("MQALQT", PKDLQT);
        lockedResult.set("MQPAQT", PKDLQT);
        lockedResult.set("MQALQN", 0D);
        // Catchweight
        if (lockedResult.getDouble("MQCAWE") != 0 && MMCAWP == 0) {
          lockedResult.set("MQCAWE", RCAWE(lockedResult));
        }
        // ACP no longer used
        lockedResult.set("MQTRQA", 0D);
        lockedResult.set("MQALQA", 0D);
        lockedResult.set("MQPAQA", 0D);
        // Adjust existing allocation line
        lockedResult.update();
        DBAction queryMITALO = database.table("MITALO")
          .index("00")
          .build();
        DBContainer containerMITALO = queryMITALO.getContainer();
        containerMITALO.set("MQCONO", XXCONO.toInteger())
        containerMITALO.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
        containerMITALO.set("MQPLSX", recMITALO.getInt("MQPLSX"));
        containerMITALO.set("MQCAMU", recMITALO.getString("MQCAMU"));
        containerMITALO.set("MQITNO", recMITALO.getString("MQITNO"));
        containerMITALO.set("MQWHLO", recMITALO.getString("MQWHLO"));
        containerMITALO.set("MQWHSL", recMITALO.getString("MQWHSL"));
        containerMITALO.set("MQBANO", recMITALO.getString("MQBANO"));
        containerMITALO.set("MQTTYP", recMITALO.getInt("MQTTYP"));
        containerMITALO.set("MQRIDN", recMITALO.getString("MQRIDN"));
        containerMITALO.set("MQRIDO", recMITALO.getInt("MQRIDO"));
        containerMITALO.set("MQRIDL", recMITALO.getInt("MQRIDL"));
        containerMITALO.set("MQRIDX", recMITALO.getInt("MQRIDX"));
        containerMITALO.set("MQSOFT", recMITALO.getInt("MQSOFT"));
        // Retrieve reporting number
        containerMITALO.set("MQPLRN", PLRN());
        // Next pick list suffix
        containerMITALO.set("MQPLSX", recMHPICH.getInt("PIPLSX"));
        containerMITALO.set("MQTRQT", 0D);
        containerMITALO.set("MQALQT", 0D);
        containerMITALO.set("MQPAQT", 0D);
        containerMITALO.set("MQALQN", 0D);
        containerMITALO.set("MQCAWE", 0D);
        // Initialise next allocation line
        splitPerformed = true;
        queryMITALO.insert(containerMITALO);
      }
    }
    DBAction queryMITALO = database.table("MITALO")
      .index("00")
      .build();
    DBContainer containerMITALO = queryMITALO.getContainer();
    containerMITALO.set("MQCONO", XXCONO.toInteger())
    containerMITALO.set("MQRIDI", Long.parseLong(recMITALO.get("MQRIDI").toString().trim()));
    containerMITALO.set("MQPLSX", recMITALO.getInt("MQPLSX"));
    containerMITALO.set("MQCAMU", recMITALO.getString("MQCAMU"));
    containerMITALO.set("MQITNO", recMITALO.getString("MQITNO"));
    containerMITALO.set("MQWHLO", recMITALO.getString("MQWHLO"));
    containerMITALO.set("MQWHSL", recMITALO.getString("MQWHSL"));
    containerMITALO.set("MQBANO", recMITALO.getString("MQBANO"));
    containerMITALO.set("MQTTYP", recMITALO.getInt("MQTTYP"));
    containerMITALO.set("MQRIDN", recMITALO.getString("MQRIDN"));
    containerMITALO.set("MQRIDO", recMITALO.getInt("MQRIDO"));
    containerMITALO.set("MQRIDL", recMITALO.getInt("MQRIDL"));
    containerMITALO.set("MQRIDX", recMITALO.getInt("MQRIDX"));
    containerMITALO.set("MQSOFT", recMITALO.getInt("MQSOFT"));
    queryMITALO.readLock(containerMITALO, updateMITALO);
  }

  /**
   *    PLRN  - Retrieve reporting number
   */
  def String PLRN() {
    String PXNBNR = "";
    def params = ["NBTY": "S1", "NBID": "A"];
    Closure<?> handler = {Map<String,String> response ->
      if (response.error == null) {
        PXNBNR = response.NBNR;
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", params, handler);
  }

  /**
   *    CPTM  - Calculate pickingtime
   */
  public void CPTM(DBContainer recMITALO, double O0DLQT) {
    //   Retrieve stock location information
    String MQWHLO = recMITALO.getString("MQWHLO").trim();
    String MQWHSL = recMITALO.getString("MQWHSL").trim();
    String MQITNO = recMITALO.getString("MQITNO").trim();
    String MSWHLT = "";
    DBAction queryMITPCE = database.table("MITPCE")
      .index("00")
      .selection("MSWHLT")
      .build();
    DBContainer MITPCE = queryMITPCE.getContainer();
    MITPCE.set("MSCONO", XXCONO.toInteger());
    MITPCE.set("MSWHLO", MQWHLO);
    MITPCE.set("MSWHSL", MQWHSL);
    if (queryMITPCE.read(MITPCE)) {
      MSWHLT = MITPCE.getString("MSWHLT").trim();
    }
    //   Retrieve item picking time
    double M6PLTM = 0D;
    double M6CTCD = 0D;
    double M6PLTS = 0D;
    char M6PTUN = ' ';
    def queryMITPTI = database.table("MITPTI")
      .index("00")
      .reverse()
      .selection("M6PLTM", "M6CTCD", "M6PLTS", "M6PTUN");

    def actionMITPTI = queryMITPTI.build();
    def MITPTI = actionMITPTI.createContainer();
    MITPTI.set("M6CONO", XXCONO.toInteger());
    MITPTI.set("M6WHLO", MQWHLO);
    MITPTI.set("M6GRTY", 1);
    MITPTI.set("M6GRIT", MQITNO);
    MITPTI.set("M6WHLT", MSWHLT);
    if (packageCheck) {
      MITPTI.set("M6MUQT", O0DLQT);
    } else {
      MITPTI.set("M6MUQT", recMITALO.getDouble("MQTRQT"));
    }
    //   1:st - For item on actual location type
    int recCntMITPTI = actionMITPTI.readAll(MITPTI, 5, 1, { DBContainer record ->
      M6PLTM = record.getDouble("M6PLTM");
      M6CTCD = record.getDouble("M6CTCD");
      M6PLTS = record.getDouble("M6PLTS");
      M6PTUN = record.getChar("M6PTUN");
    });
    if (recCntMITPTI == 0) {
      //   2:nd - For item without location type
      MITPTI.set("M6WHLT", "");
      recCntMITPTI = actionMITPTI.readAll(MITPTI, 5, 1, { DBContainer record ->
        M6PLTM = record.getDouble("M6PLTM");
        M6CTCD = record.getDouble("M6CTCD");
        M6PLTS = record.getDouble("M6PLTS");
        M6PTUN = record.getChar("M6PTUN");
      });
    }
    if (recCntMITPTI == 0) {
      //   3:rd - For distr group technology on actual location type
      MITPTI.set("M6GRTY", 2);
      MITPTI.set("M6GRIT", MMGRTS);
      MITPTI.set("M6WHLT", MSWHLT);
      recCntMITPTI = actionMITPTI.readAll(MITPTI, 5, 1, { DBContainer record ->
        M6PLTM = record.getDouble("M6PLTM");
        M6CTCD = record.getDouble("M6CTCD");
        M6PLTS = record.getDouble("M6PLTS");
        M6PTUN = record.getChar("M6PTUN");
      });
    }
    if (recCntMITPTI == 0) {
      //   4:th - For distr group technology without location type
      MITPTI.set("M6WHLT", "");
      recCntMITPTI = actionMITPTI.readAll(MITPTI, 5, 1, { DBContainer record ->
        M6PLTM = record.getDouble("M6PLTM");
        M6CTCD = record.getDouble("M6CTCD");
        M6PLTS = record.getDouble("M6PLTS");
        M6PTUN = record.getChar("M6PTUN");
      });
    }
    if (recCntMITPTI == 0) {
      //   5:th - On actual location type
      MITPTI.set("M6GRTY", 3);
      MITPTI.set("M6GRIT", "");
      MITPTI.set("M6WHLT", MSWHLT);
      recCntMITPTI = actionMITPTI.readAll(MITPTI, 5, 1, { DBContainer record ->
        M6PLTM = record.getDouble("M6PLTM");
        M6CTCD = record.getDouble("M6CTCD");
        M6PLTS = record.getDouble("M6PLTS");
        M6PTUN = record.getChar("M6PTUN");
      });
    }
    if (recCntMITPTI == 0) {
      //   6:th - Without location type
      MITPTI.set("M6GRTY", 3);
      MITPTI.set("M6GRIT", "");
      MITPTI.set("M6WHLT", "");
      recCntMITPTI = actionMITPTI.readAll(MITPTI, 5, 1, { DBContainer record ->
        M6PLTM = record.getDouble("M6PLTM");
        M6CTCD = record.getDouble("M6CTCD");
        M6PLTS = record.getDouble("M6PLTS");
        M6PTUN = record.getChar("M6PTUN");
      });
    }
    //   No item picking time found
    if (recCntMITPTI == 0) {
      return;
    }
    //   Calculate item picking time
    if (packageCheck) {
      XXNUM = O0DLQT * M6PLTM;
    } else {
      XXNUM = recMITALO.getDouble("MQTRQT") * M6PLTM;
    }
    //   Consider Price and time quantity
    if (M6CTCD != 0) {
      XXNUM /= M6CTCD;
    }
    //   Add setup time
    XXNUM += M6PLTS;
    //   Consider Time U/m
    switch (M6PTUN) {
      case '2':
        //   Minutes with 2 dec => Hours with 2 dec
        XXNUM /= 60d;
        break;
      case '3':
        //   Seconds with 2 dec => Hours with 2 dec
        XXNUM /= 3600d;
        break;
      default:
        //   Already in Hours with 2 dec
        break;
    }
  }

  /**
   *    CASPLT  - Calculate allocation based split
   */
  def void CASPLT(DBContainer recMHPICH) {
    // Level breaks
    breakOnLine = true;
    breakOnTime = false;
    breakOnWeight = false;
    breakOnVolume = false;
    lineUpdate = false;
    // Work flow
    splitPerformed = false;

    int orgITALOPLSX = 0;
    boolean restoreValues = false;
    // Allocations
    List<DBContainer> lstMITALO30 = readMITALO30(recMHPICH);
    for (DBContainer recMITALO30: lstMITALO30) {
      orgITALOPLSX = recMITALO30.getInt("MQPLSX");
      LINCNT++;
      TOTCNT++;
      logger.debug("CASPLT:checkPickLines=${checkPickLines}/LINCNT=${MCMXPL}/lineUpdate=${lineUpdate}/XXNPLL=${XXNPLL}");
      // Perform line break check
      if (checkPickLines &&
        LINCNT > MCMXPL) {
        XXPCSI = 15;
        lineUpdate = true;
        RSPLIT(recMHPICH, recMITALO30);
      } else {
        // Break already hit - update all subsequent lines
        if (lineUpdate) {
          updSubLines(recMITALO30, recMHPICH);
        }
      }
      // Perform accumulations
      restoreValues = false;
      //   Retrieve item
      getItemDtls(recMITALO30.getString("MQITNO"));
      logger.debug("CASPLT:EDCPTM=${EDCPTM}");
      if (EDCPTM == 1) {
        CPTM(recMITALO30, 0);
        TIMCNT += XXNUM;
        TIMWCT += XXNUM;
      }
      XXGRWE = MMGRWE * recMITALO30.getDouble("MQTRQT");
      XXVOL3 = MMVOL3 * recMITALO30.getDouble("MQTRQT");
      PKGRWE += XXGRWE;
      PKVOL3 += XXVOL3;
      // Weight
      logger.debug("CASPLT:checkItemWeight=${checkItemWeight}/PKGRWE=${PKGRWE}/MCMXPW=${MCMXPW}");
      if (checkItemWeight) {
        if (PKGRWE > MCMXPW) {
          XXPCSI = 12;
          lineUpdate = true;
          breakOnWeight = true;
          // Need to check if breaking on a single line
          if (LINCNT > 1) {
            restoreValues = true;
            // Remove latest totals and update retrospectively
            TIMCNT -= XXNUM;
            PKGRWE -= XXGRWE;
            PKVOL3 -= XXVOL3;
          }
          // Create next suffix and prime next MITALO record
          RSPLIT(recMHPICH, recMITALO30);
          if (restoreValues) {
            restoreValues = false;
            // Restore current totals
            TIMCNT += XXNUM;
            PKGRWE += XXGRWE;
            PKVOL3 += XXVOL3;
          }
        }
      }
      // Volume
      logger.debug("CASPLT:checkItemVolume=${checkItemVolume}/breakOnWeight=${breakOnWeight}/PKVOL3=${PKVOL3}/MCMXPV=${MCMXPV}")
      if (checkItemVolume && !breakOnWeight) {
        if (PKVOL3 > MCMXPV) {
          XXPCSI = 13;
          lineUpdate = true;
          breakOnVolume = true;
          // Need to check if breaking on a single line
          if (LINCNT > 1) {
            restoreValues = true;
            // Remove latest totals and update retrospectively
            TIMCNT -= XXNUM;
            PKGRWE -= XXGRWE;
            PKVOL3 -= XXVOL3;
          }
          // Create next suffix and prime next MITALO record
          RSPLIT(recMHPICH, recMITALO30);
          if (restoreValues) {
            restoreValues = false;
            // Restore current totals
            TIMCNT += XXNUM;
            PKGRWE += XXGRWE;
            PKVOL3 += XXVOL3;
          }
        }
      }
      // Pick time
      if (checkPickTime &&
        !breakOnWeight &&
        !breakOnVolume &&
        EDCPTM == 1) {
        if (TIMCNT > MCMXPT) {
          XXPCSI = 14;
          lineUpdate = true;
          breakOnTime = true;
          // Need to check if breaking on a single package
          if (LINCNT > 1) {
            restoreValues = true;
            // Remove latest totals and update retrospectively
            TIMCNT -= XXNUM;
            PKGRWE -= XXGRWE;
            PKVOL3 -= XXVOL3;
          }
          // Create next suffix and prime next MITALO record
          RSPLIT(recMHPICH, recMITALO30);
          if (restoreValues) {
            restoreValues = false;
            // Restore current totals
            TIMCNT += XXNUM;
            PKGRWE += XXGRWE;
            PKVOL3 += XXVOL3;
          }
        }
      }
      breakOnWeight = false;
      breakOnVolume = false;
      breakOnTime = false;
    }
    //   Remove current picking list if all picking list lines moved to new picking list
    List<DBContainer> listMITALO30 = readMITALO30_MQPLSX(recMHPICH, orgITALOPLSX);
    if (listMITALO30.size() == 0) {
      delMHPICH(recMHPICH, orgITALOPLSX);
    } else {
      DBAction query = database.table("MHPICH")
        .index("00")
        .reverse()
        .selectAllFields()
        .build();
      DBContainer container = query.getContainer();
      container.set("PICONO", XXCONO.toInteger());
      container.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
      boolean isFirstRecord = true;
      query.readAll(container, 2, 1,{ DBContainer record ->
        if (isFirstRecord) {
          recMHPICH = record;
          isFirstRecord = false;
        }
      });
      logger.debug("containers=====${recMHPICH}");
      // Update last MHPICH
      if (lineUpdate && LINCNT > 0) {
        Closure<?> updateMHPICH = { LockedResult lockedResult ->
          lockedResult.set("PINPLL", LINCNT);
          lockedResult.set("PIPLTM", TIMCNT);
          lockedResult.set("PILMDT", currentDate);
          lockedResult.set("PICHID", program.getUser());
          lockedResult.set("PICHNO", lockedResult.getInt("PICHNO") + 1);
          lockedResult.update();
        }
        DBAction queryMHPICH = database.table("MHPICH")
          .index("00")
          .build();
        DBContainer containerMHPICH = queryMHPICH.getContainer()
        containerMHPICH.set("PICONO", XXCONO.toInteger());
        containerMHPICH.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
        containerMHPICH.set("PIPLSX", recMHPICH.getInt("PIPLSX"));
        queryMHPICH.readLock(containerMHPICH, updateMHPICH);
      }
      // Update last MHPICH - execute even if no split performed
      updLastMHPICH_CASPLT(recMHPICH);
      if (EDPRTA == 1 && !picklistPrintJob) {
        PRPIC(recMHPICH);
      }
    }
  }

  /**
   * updLastMHPICH_CASPLT - update MHPICH for CASPLT method
   * @param recMHPICH
   * @return
   */
  def updLastMHPICH_CASPLT(DBContainer recMHPICH) {
    Closure<?> updateMHPICH = { LockedResult lockedResult ->
      lockedResult.set("PIMXPW", MCMXPW);
      lockedResult.set("PIMXPV", MCMXPV);
      lockedResult.set("PIMXPT", MCMXPT);
      lockedResult.set("PIMXPL", MCMXPL);
      lockedResult.set("PIMXPP", 0);
      lockedResult.set("PICLPP", 0);
      if (checkItemWeight) {
        lockedResult.set("PICLPW", PKGRWE);
      } else {
        lockedResult.set("PICLPW", 0d);
      }
      if (checkItemVolume) {
        lockedResult.set("PICLPV", PKVOL3);
      } else {
        lockedResult.set("PICLPV", 0d);
      }
      if (checkPickTime) {
        lockedResult.set("PICLPT", TIMCNT);
      } else {
        lockedResult.set("PICLPT", 0d);
      }
      if (checkPickLines) {
        lockedResult.set("PICLPL", LINCNT);
      } else {
        lockedResult.set("PICLPL", 0);
      }
      lockedResult.set("PILMDT", currentDate);
      lockedResult.set("PICHID", program.getUser());
      lockedResult.set("PICHNO", lockedResult.getInt("PICHNO") + 1);
      lockedResult.update();
      // Print pick list
      if (EDPRTA == 1 && EDPIRL == 2) {
        PRPIC(lockedResult);
      }
    }
    DBAction queryMHPICH = database.table("MHPICH")
      .index("00")
      .build();
    DBContainer containerMHPICH = queryMHPICH.getContainer()
    containerMHPICH.set("PICONO", XXCONO.toInteger());
    containerMHPICH.set("PIDLIX", recMHPICH.getLong("PIDLIX"));
    containerMHPICH.set("PIPLSX", recMHPICH.getInt("PIPLSX"));
    queryMHPICH.readLock(containerMHPICH, updateMHPICH);
  }
}
