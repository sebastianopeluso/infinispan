package eu.cloudtm.rmi.statistics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class CustomMis implements Serializable{
	
	private static final long serialVersionUID = -2668581768751397122L;
	
	//private String nome;
	private int valore;
	
	//public String getNome() {
	//	return nome;
	//}
	//public void setNome(String nome) {
	//	this.nome = nome;
	//	}
	public int getValore() {
		return valore;
	}
	public void setValore(int valore) {
		this.valore = valore;
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException{
		out.defaultWriteObject();
		out.writeObject(new Integer(valore));
		System.out.println("pippo");
	}
	private void readObject(ObjectInputStream aStream) throws IOException, ClassNotFoundException {
		aStream.defaultReadObject();
		this.valore = (Integer)aStream.readObject();
	}
	
}
