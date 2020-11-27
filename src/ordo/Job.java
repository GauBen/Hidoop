package ordo;

import application.MyMapReduce;
import formats.Format;
import map.MapReduce;

public class Job implements JobInterfaceX {
    //TODO



    @Override
    public void setInputFormat(Format.Type ft) {

    }

    @Override
    public void setInputFname(String fname) {

    }

    @Override
    public void startJob(MapReduce mr) {

    }

    @Override
    public void setNumberOfReduces(int tasks) {

    }

    @Override
    public void setNumberOfMaps(int tasks) {

    }

    @Override
    public void setOutputFormat(Format.Type ft) {

    }

    @Override
    public void setOutputFname(String fname) {

    }

    @Override
    public void setSortComparator(SortComparator sc) {

    }

    @Override
    public int getNumberOfReduces() {
        return 0;
    }

    @Override
    public int getNumberOfMaps() {
        return 0;
    }

    @Override
    public Format.Type getInputFormat() {
        return null;
    }

    @Override
    public Format.Type getOutputFormat() {
        return null;
    }

    @Override
    public String getInputFname() {
        return null;
    }

    @Override
    public String getOutputFname() {
        return null;
    }

    @Override
    public SortComparator getSortComparator() {
        return null;
    }
}
