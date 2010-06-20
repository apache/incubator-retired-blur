package using;


public class ExExp {
	
	public static final Exception EXCEPTION = new Exception();
	public static final Object OBJECT = new Object();

	public static void main(String[] args) {
		try {
			normal();
		} catch (Exception e) {
		}
		try {
			error();
		} catch (Exception e) {
		}
		long count = 0;
		long normalTotal = 0;
		for (int i = 0; i < 100000000; i++) {
			long s = System.nanoTime();
			try {
				if (normal() != null) {
					count++;
				}
			} catch (Exception e) {
			}
			long e = System.nanoTime();
			normalTotal += (e-s);
		}
		
		long count2 = 0;
		long errorTotal = 0;
		for (int i = 0; i < 100000000; i++) {
			long s = System.nanoTime();
			try {
				error();
			} catch (Exception e) {
				count2++;
			}
			long e = System.nanoTime();
			errorTotal += (e-s);
		}

		System.out.println(normalTotal);
		System.out.println(errorTotal);
	}
	
	public static Object normal() throws Exception {
		return OBJECT;
	}
	
	public static Object error() throws Exception {
		throw EXCEPTION;
	}

}
