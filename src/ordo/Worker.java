package ordo;

import formats.Format;
import map.FileLessMapperReducer;
import map.Mapper;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Worker extends Remote {
    void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;

    void runFileLessMap(FileLessMapperReducer m, HidoopTask task, Format writer, CallBack cb) throws RemoteException;

    int getNumberOfCores() throws RemoteException;
}
