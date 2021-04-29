package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import map.FileLessMapperReducer;
import map.Mapper;
import formats.Format;

public interface Worker extends Remote {
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;

    public void runFileLessMap(FileLessMapperReducer m, HidoopTask task, Format writer, CallBack cb) throws RemoteException;

    public int getNumberOfCores() throws RemoteException;
}
