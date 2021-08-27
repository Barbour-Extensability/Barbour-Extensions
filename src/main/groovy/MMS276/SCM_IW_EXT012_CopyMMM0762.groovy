/**
 * README
 * This extension is being used to copy work table MMM076 to EXTFOS
 *
 * Name: SCM_IW_EXT012_CopyMMM0762
 * Description: Replicate MMM076 from MMS276
 * Date	      Changed By           	          Description
 * 20210803   Franco Cortez                   Replicate MMM076 from MMS276
 * 20210812   Franco Cortez                   Change - added color column and write records with size color combination
 */
public class SCM_IW_EXT012_CopyMMM0762 extends ExtendM3Trigger {
  private final LoggerAPI logger;
  private final ProgramAPI program;
  private final DatabaseAPI database;
  private final InteractiveAPI interactive
  private List listXs;
  private List listYs;
  private boolean hasX = false;

  public SCM_IW_EXT012_CopyMMM0762(LoggerAPI logger, ProgramAPI program, DatabaseAPI database, InteractiveAPI interactive) {
    this.logger = logger;
    this.program = program;
    this.database = database;
    this.interactive = interactive;
  }

  public void main() {
    logger.debug("Test PBCHK");
    int ZZCONO = program.LDAZZ.CONO;
    String ZZSTYN = interactive.display.fields.WWSTYN;
    logger.debug("Test PBCHK === ZZCONO: " + ZZCONO);
    logger.debug("Test PBCHK === ZZSTYN: " + ZZSTYN);

    //Delete All STYN from EXTFOS
    ArrayList < String[] > listEXTFOS = new ArrayList < String[] > ();
    Iterator < String[] > listEXTFOSIterator = null;

    DBAction dbaEXTFOS = database.table("EXTFOS").index("00").selection("EXCONO", "EXSTYN", "EXFGRP", "EXFTID", "EXOPTN", "EXOPTY").build();
    DBContainer conEXTFOS = dbaEXTFOS.getContainer();
    conEXTFOS.set("EXCONO", ZZCONO);
    conEXTFOS.set("EXSTYN", ZZSTYN);

    Closure <  ?  > resultHandlerEXTFOS = {
      DBContainer data->
        String[]currResult = [
          data.get("EXFGRP").toString(),
          data.get("EXFTID").toString(),
          data.get("EXOPTN").toString(),
          data.get("EXOPTY").toString()];

        listEXTFOS.add(currResult);
    }
    dbaEXTFOS.readAll(conEXTFOS, 2, resultHandlerEXTFOS);
    listEXTFOSIterator = listEXTFOS.iterator();
    while (listEXTFOSIterator.hasNext()) {
      String[]currResult = listEXTFOSIterator.next();
      DBAction dbaEXTFOSDEL = database.table("EXTFOS").index("00").selection("EXCONO", "EXSTYN", "EXFGRP", "EXFTID", "EXOPTN", "EXOPTY").build();
      DBContainer conEXTFOSDEL = dbaEXTFOSDEL.getContainer();
      conEXTFOSDEL.set("EXCONO", ZZCONO);
      conEXTFOSDEL.set("EXSTYN", ZZSTYN);
      conEXTFOSDEL.set("EXFGRP", currResult[0]);
      conEXTFOSDEL.set("EXFTID", currResult[1]);
      conEXTFOSDEL.set("EXOPTN", currResult[2]);
      conEXTFOSDEL.set("EXOPTY", currResult[3]);

      if (dbaEXTFOSDEL.read(conEXTFOSDEL)) {
        Closure <  ?  > deleteCallback = {
          LockedResult lockedResult->
            lockedResult.delete();
        }
        dbaEXTFOSDEL.readLock(conEXTFOSDEL, deleteCallback);
      }
    }
    // Check if no FGRP == "X"
    listXs = new ArrayList();
    DBAction queryMMM076X = database.table("MMM076").index("00").selection("HHCONO", "HHSTYN", "HHFGRP", "HHFTID", "HHOPTN").build();
    DBContainer MMM076X = queryMMM076X.getContainer();
    MMM076X.set("HHCONO", ZZCONO);
    MMM076X.set("HHSTYN", ZZSTYN);
    MMM076X.set("HHFGRP", "X");

    Closure <  ?  > listMMM076X = {
      DBContainer data->
        listXs.add(data.get("HHOPTN").toString().trim());
    }

    queryMMM076X.readAll(MMM076X, 3, listMMM076X);
    logger.debug("listXs.size=" + listXs.size());
    if (listXs.size() > 0) {
      hasX = true;
    }
    if (hasX) {
      //Collect all Ys then loop through Xs to combine with Ys.
      listYs = new ArrayList();
      DBAction queryMMM076Y = database.table("MMM076").index("00").selection("HHCONO", "HHSTYN", "HHFGRP", "HHFTID", "HHOPTN").build();
      DBContainer MMM076Y = queryMMM076Y.getContainer();
      MMM076Y.set("HHCONO", ZZCONO);
      MMM076Y.set("HHSTYN", ZZSTYN);
      MMM076Y.set("HHFGRP", "Y");

      Closure <  ?  > listMMM076Y = {
        DBContainer data->
          listYs.add(data.get("HHOPTN").toString().trim());
      }

      queryMMM076Y.readAll(MMM076Y, 3, listMMM076Y);
      logger.debug("listYs.size=" + listYs.size());

      if (listYs.size() == 0) {
        listYs.add("");
      }

      for (int i = 0; i < listYs.size(); i++) {
        String opty = listYs[i].toString();
        logger.debug("listYs.record no=" + i);
        logger.debug("listYs.opty=" + opty);
        //List MMM076 with STYN selected and Write each record to EXTFOS if all FGRP = "Y".
        ArrayList < String[] > listMMM076 = new ArrayList < String[] > ();
        Iterator < String[] > listMMM076Iterator = null;

        DBAction dbaMMM076 = database.table("MMM076").index("00").selection("HHCONO", "HHSTYN", "HHFGRP", "HHFTID", "HHOPTN", "HHTX15", "HHTX30", "HHTXFE",
          "HHSQFU", "HHSQNU", "HHRGDT", "HHRGTM", "HHLMDT", "HHCHNO", "HHCHID", "HHSEA1").build();
        DBContainer conMMM076 = dbaMMM076.getContainer();
        conMMM076.set("HHCONO", ZZCONO);
        conMMM076.set("HHSTYN", ZZSTYN);
        conMMM076.set("HHFGRP", "X");

        Closure <  ?  > resultHandlerMMM076 = {
          DBContainer data->
            String[]currResult = [
              data.get("HHFGRP").toString(),
              data.get("HHFTID").toString(),
              data.get("HHOPTN").toString(),
              data.get("HHTX15").toString(),
              data.get("HHTX30").toString(),
              data.get("HHTXFE").toString(),
              data.get("HHSQFU").toString(),
              data.get("HHSQNU").toString(),
              data.get("HHRGDT").toString(),
              data.get("HHRGTM").toString(),
              data.get("HHLMDT").toString(),
              data.get("HHCHNO").toString(),
              data.get("HHCHID").toString(),
              data.get("HHSEA1").toString()];

            listMMM076.add(currResult);
        }
        dbaMMM076.readAll(conMMM076, 3, resultHandlerMMM076);
        listMMM076Iterator = listMMM076.iterator();
        while (listMMM076Iterator.hasNext()) {
          String[]currResult = listMMM076Iterator.next();
          //Copy record from MMM076 to EXTFOS
          DBAction dbaEXTFOSADD = database.table("EXTFOS").index("00").selection("EXCONO", "EXSTYN", "EXFGRP", "EXFTID", "EXOPTN", "EXOPTY").build();
          DBContainer conEXTFOSADD = dbaEXTFOSADD.getContainer();
          conEXTFOSADD.set("EXCONO", ZZCONO);
          conEXTFOSADD.set("EXSTYN", ZZSTYN);
          conEXTFOSADD.set("EXFGRP", currResult[0]);
          conEXTFOSADD.set("EXFTID", currResult[1]);
          conEXTFOSADD.set("EXOPTN", currResult[2]);

          if (dbaEXTFOSADD.read(conEXTFOSADD)) {
            //dummy
          } else {
            conEXTFOSADD.set("EXTX15", currResult[3]);
            conEXTFOSADD.set("EXTX30", currResult[4]);
            conEXTFOSADD.set("EXTXFE", currResult[5]);
            conEXTFOSADD.set("EXSQFU", currResult[6].toInteger());
            conEXTFOSADD.set("EXSQNU", currResult[7].toInteger());
            conEXTFOSADD.set("EXRGDT", currResult[8].toInteger());
            conEXTFOSADD.set("EXRGTM", currResult[9].toInteger());
            conEXTFOSADD.set("EXLMDT", currResult[10].toInteger());
            conEXTFOSADD.set("EXCHNO", currResult[11].toInteger());
            conEXTFOSADD.set("EXCHID", currResult[12]);
            conEXTFOSADD.set("EXSEA1", currResult[13]);
            conEXTFOSADD.set("EXOPTY", opty);

            Closure <  ?  > insertCallBack = {};
            dbaEXTFOSADD.insert(conEXTFOSADD, insertCallBack);
          }
        }
      }
    } else {
      //List MMM076 with STYN selected and Write each record to EXTFOS if all FGRP = "Y".
      ArrayList < String[] > listMMM076 = new ArrayList < String[] > ();
      Iterator < String[] > listMMM076Iterator = null;

      DBAction dbaMMM076 = database.table("MMM076").index("00").selection("HHCONO", "HHSTYN", "HHFGRP", "HHFTID", "HHOPTN", "HHTX15", "HHTX30", "HHTXFE",
        "HHSQFU", "HHSQNU", "HHRGDT", "HHRGTM", "HHLMDT", "HHCHNO", "HHCHID", "HHSEA1").build();
      DBContainer conMMM076 = dbaMMM076.getContainer();
      conMMM076.set("HHCONO", ZZCONO);
      conMMM076.set("HHSTYN", ZZSTYN);

      Closure <  ?  > resultHandlerMMM076 = {
        DBContainer data->
          String[]currResult = [
            data.get("HHFGRP").toString(),
            data.get("HHFTID").toString(),
            data.get("HHOPTN").toString(),
            data.get("HHTX15").toString(),
            data.get("HHTX30").toString(),
            data.get("HHTXFE").toString(),
            data.get("HHSQFU").toString(),
            data.get("HHSQNU").toString(),
            data.get("HHRGDT").toString(),
            data.get("HHRGTM").toString(),
            data.get("HHLMDT").toString(),
            data.get("HHCHNO").toString(),
            data.get("HHCHID").toString(),
            data.get("HHSEA1").toString()];

          listMMM076.add(currResult);
      }
      dbaMMM076.readAll(conMMM076, 2, resultHandlerMMM076);
      listMMM076Iterator = listMMM076.iterator();
      while (listMMM076Iterator.hasNext()) {
        String[]currResult = listMMM076Iterator.next();
        //Copy record from MMM076 to EXTFOS
        DBAction dbaEXTFOSADD = database.table("EXTFOS").index("00").selection("EXCONO", "EXSTYN", "EXFGRP", "EXFTID", "EXOPTN", "EXOPTY").build();
        DBContainer conEXTFOSADD = dbaEXTFOSADD.getContainer();
        conEXTFOSADD.set("EXCONO", ZZCONO);
        conEXTFOSADD.set("EXSTYN", ZZSTYN);
        conEXTFOSADD.set("EXFGRP", currResult[0]);
        conEXTFOSADD.set("EXFTID", currResult[1]);
        conEXTFOSADD.set("EXOPTN", currResult[2]);

        if (dbaEXTFOSADD.read(conEXTFOSADD)) {
          //dummy
        } else {
          conEXTFOSADD.set("EXTX15", currResult[3]);
          conEXTFOSADD.set("EXTX30", currResult[4]);
          conEXTFOSADD.set("EXTXFE", currResult[5]);
          conEXTFOSADD.set("EXSQFU", currResult[6].toInteger());
          conEXTFOSADD.set("EXSQNU", currResult[7].toInteger());
          conEXTFOSADD.set("EXRGDT", currResult[8].toInteger());
          conEXTFOSADD.set("EXRGTM", currResult[9].toInteger());
          conEXTFOSADD.set("EXLMDT", currResult[10].toInteger());
          conEXTFOSADD.set("EXCHNO", currResult[11].toInteger());
          conEXTFOSADD.set("EXCHID", currResult[12]);
          conEXTFOSADD.set("EXSEA1", currResult[13]);

          Closure <  ?  > insertCallBack = {};

          dbaEXTFOSADD.insert(conEXTFOSADD, insertCallBack);
        }
      }
    }

  }
}
