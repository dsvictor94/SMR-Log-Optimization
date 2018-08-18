package ch.usi.da.smr;

public enum WorkloadType {
	   READ(1),
	   WRITE(2);
	   
	   private int value;
	   private WorkloadType(int value) {
	      this.value = value;
	   }
	   public int getValue() {
	      return value;
	   }	   
	 
	}
