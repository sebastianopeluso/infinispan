package eu.cloudtm.rmi.statistics;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: diego
 * Date: 26/12/11
 * Time: 20:35
 * To change this template use File | Settings | File Templates.
 */
public interface ISPNStats {
    //The following methods are implemented both by the threadStats and the NodeStats

    //IN ORDER TO AVOID METHODS EXPLOSION, EVERY ELEMENT WHICH ACCESSES A ISPNSTATS HAS TO BE AWARE OF THE BINDING
    //INDEX<->PARAMETER



    double getParameter(int index);
    void setParameter(int index, double value);
    void addParameter(int index, double delta);
    void reset();
    //List<Object> getObjects(int index);


}
