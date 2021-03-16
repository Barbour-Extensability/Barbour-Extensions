/**
 * README
 * This extension is being used in Mongoose
 *
 * Name: EXT008MI.Release
 * Description: Release pick list
 * Date	      Changed By           	          Description
 * 20210311   Edison Villar                   INT115 – Maintain Commodity Code across Facilities
 */


public class CascadeCSNO extends ExtendM3Transaction {
  private final MIAPI mi
  private final MICallerAPI miCaller
  private final ProgramAPI program

  private String inFACI, inITNO, inCSNO
  private String XXCONO

  public CascadeCSNO(MIAPI mi, MICallerAPI miCaller, ProgramAPI program) {
    this.mi = mi
    this.miCaller = miCaller
    this.program = program
  }

  public void main() {
    List<String> lstFACI = new ArrayList<String>(); //List of facilities

    // - Get input values
    inFACI = mi.inData.get("FACI") == null ? "" : mi.inData.get("FACI").trim()
    inITNO = mi.inData.get("ITNO") == null ? "" : mi.inData.get("ITNO").trim()
    inCSNO = mi.inData.get("CSNO") == null ? "" : mi.inData.get("CSNO").trim()
    // - Set company
    XXCONO = program.LDAZD.CONO.toString()

    // - Validate required inputs
    if("".equals(inFACI)){
      mi.error("Facility is mandatory")
    }

    if(!"100".equals(inFACI)){   //Should be 100
      mi.error("Facility must be 100")
    }

    if("".equals(inITNO)){
      mi.error("Item Number is mandatory")
    }


    // - Execute MMS200MI.LstFacByItem
    // To list all facilities having item = xITNO
    def lstParam = ["ITNO": inITNO]
    Closure<?> lstHandler = {Map<String,String> response ->
      if(response.error){
        mi.error(response.errorMessage)
      } else {
        if (response.CSNO != inCSNO) {
          lstFACI.add(response.FACI)
        }
      }
    }

    miCaller.call("MMS200MI", "LstFacByItem", lstParam, lstHandler);

    //Obtain CSNO of ITNO-FACILITY pair
    Closure<?> getHandler = {Map<String,String> response ->
      if(response.error){
        mi.error(response.errorMessage)
      } else {
        if (response.CSNO == inCSNO) {
          lstFACI.remove(response.FACI)  //remove from the list as there is no need to update CSNO in the said facility
        }
      }
    }

    for (String faci: lstFACI)
    {
      def getParam = ["ITNO": inITNO]
      miCaller.call("MMS200MI", "GetItmFac", getParam, getHandler)
    }


    //Now loop again on the facilities to update CSNO
    Closure<?> updHandler = { Map<String,String> response ->
      if(response.error){
        mi.error(response.errorMessage)
      } else {
        //do nothing
      }
    }

    for (String faci: lstFACI)
    {
      def updParam = ["FACI": faci, "ITNO": inITNO, "CSNO": inCSNO]
      miCaller.call("MMS200MI", "UpdItmFac", updParam, updHandler)
    }
  }
}
