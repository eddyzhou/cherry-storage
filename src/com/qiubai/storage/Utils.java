package com.qiubai.storage;

public class Utils {
	private Utils() {
	}

	public static int getlargerPrime(int n) {
		if (n >= 1610612741) {
			throw new IllegalArgumentException("n must < 1610612741");
		}
		int[] prime = { 53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593,
				49157, 98317, 196613, 393241, 786433, 1572869, 3145739,
				6291469, 12582917, 25165843, 50331653, 100663319, 201326611,
				402653189, 805306457, 1610612741 };
		for (int i = 0; i < prime.length; i++) {
			if (prime[i] > n)
				return prime[i];
		}
		return 0;
	}

}
