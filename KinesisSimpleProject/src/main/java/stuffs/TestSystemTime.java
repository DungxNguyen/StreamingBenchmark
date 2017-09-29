package stuffs;

public class TestSystemTime {

	public static void main(String[] args) {
		testTime();
	}
	
	public static void testTime() {
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			System.currentTimeMillis();
		}
		System.out.println(System.currentTimeMillis() - startTime);
	}
}
