/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils.bitset;

import org.apache.lucene.util.OpenBitSet;

public class BlurBitSet extends OpenBitSet {

	private static final long serialVersionUID = 8837907556056943537L;

	private static final long X1 = 0x4000000000000000l;
	private static final long X2 = 0x2000000000000000l;
	private static final long X3 = 0x1000000000000000l;
	private static final long X4 = 0x0800000000000000l;
	private static final long X5 = 0x0400000000000000l;
	private static final long X6 = 0x0200000000000000l;
	private static final long X7 = 0x0100000000000000l;
	private static final long X8 = 0x0080000000000000l;
	private static final long X9 = 0x0040000000000000l;
	private static final long X10 = 0x0020000000000000l;
	private static final long X11 = 0x0010000000000000l;
	private static final long X12 = 0x0008000000000000l;
	private static final long X13 = 0x0004000000000000l;
	private static final long X14 = 0x0002000000000000l;
	private static final long X15 = 0x0001000000000000l;
	private static final long X16 = 0x0000800000000000l;
	private static final long X17 = 0x0000400000000000l;
	private static final long X18 = 0x0000200000000000l;
	private static final long X19 = 0x0000100000000000l;
	private static final long X20 = 0x0000080000000000l;
	private static final long X21 = 0x0000040000000000l;
	private static final long X22 = 0x0000020000000000l;
	private static final long X23 = 0x0000010000000000l;
	private static final long X24 = 0x0000008000000000l;
	private static final long X25 = 0x0000004000000000l;
	private static final long X26 = 0x0000002000000000l;
	private static final long X27 = 0x0000001000000000l;
	private static final long X28 = 0x0000000800000000l;
	private static final long X29 = 0x0000000400000000l;
	private static final long X30 = 0x0000000200000000l;
	private static final long X31 = 0x0000000100000000l;
	private static final long X32 = 0x0000000080000000l;
	private static final long X33 = 0x0000000040000000l;
	private static final long X34 = 0x0000000020000000l;
	private static final long X35 = 0x0000000010000000l;
	private static final long X36 = 0x0000000008000000l;
	private static final long X37 = 0x0000000004000000l;
	private static final long X38 = 0x0000000002000000l;
	private static final long X39 = 0x0000000001000000l;
	private static final long X40 = 0x0000000000800000l;
	private static final long X41 = 0x0000000000400000l;
	private static final long X42 = 0x0000000000200000l;
	private static final long X43 = 0x0000000000100000l;
	private static final long X44 = 0x0000000000080000l;
	private static final long X45 = 0x0000000000040000l;
	private static final long X46 = 0x0000000000020000l;
	private static final long X47 = 0x0000000000010000l;
	private static final long X48 = 0x0000000000008000l;
	private static final long X49 = 0x0000000000004000l;
	private static final long X50 = 0x0000000000002000l;
	private static final long X51 = 0x0000000000001000l;
	private static final long X52 = 0x0000000000000800l;
	private static final long X53 = 0x0000000000000400l;
	private static final long X54 = 0x0000000000000200l;
	private static final long X55 = 0x0000000000000100l;
	private static final long X56 = 0x0000000000000080l;
	private static final long X57 = 0x0000000000000040l;
	private static final long X58 = 0x0000000000000020l;
	private static final long X59 = 0x0000000000000010l;
	private static final long X60 = 0x0000000000000008l;
	private static final long X61 = 0x0000000000000004l;
	private static final long X62 = 0x0000000000000002l;
	private static final long X63 = 0x0000000000000001l;

	public static void main(String[] args) {

		BlurBitSet bitSet = new BlurBitSet();
		bitSet.set(0);
		bitSet.set(3);
		bitSet.set(5);

		System.out.println(bitSet.prevSetBit(2));

		// System.out.println(bitSet.prevSetBit(65));

		for (int i = 0; i < 70; i++) {
			System.out.println(i + " " + bitSet.prevSetBit(i));
		}

	}

	public BlurBitSet() {
		super();
	}

	public BlurBitSet(long numBits) {
		super(numBits);
	}

	public BlurBitSet(long[] bits, int numWords) {
		super(bits, numWords);
	}

	public int prevSetBit(int index) {
		return (int) prevSetBit((long) index);
	}

	public long prevSetBit(long index) {
		if (index < 0) {
			return -1;
		}
		int initialWordNum = (int) (index >> 6);
		if (initialWordNum == -1) {
			initialWordNum = Integer.MAX_VALUE;
		}
		int wordNum = initialWordNum;
		if (wordNum > wlen - 1) {
			wordNum = wlen - 1;
		}
		while (wordNum >= 0) {
			long word = bits[wordNum];
			if (word != 0) {
				if (wordNum == initialWordNum) {
					int offset = ((int) (index & 0x3F));
					long mask = -1L >>> (63 - offset);
					word = word & mask;
				}
				if (word != 0) {
					return (((long) wordNum) << 6)
							+ (63 - countLeftZeros(word));
				}
			}
			wordNum--;
		}
		return -1;
	}

	static int countLeftZeros(long word) {
		if (word < -1) {
			return 0;
		}
		if (word < X32) {
			if (word < X48) {
				if (word < X56) {
					if (word < X60) {
						if (word < X62) {
							if (word < X63) {
								return 64;
							} else {
								return 63;
							}
						} else {
							if (word < X61) {
								return 62;
							} else {
								return 61;
							}
						}
					} else {
						if (word < X58) {
							if (word < X59) {
								return 60;
							} else {
								return 59;
							}
						} else {
							if (word < X57) {
								return 58;
							} else {
								return 57;
							}
						}
					}
				} else {
					if (word < X52) {
						if (word < X54) {
							if (word < X55) {
								return 56;
							} else {
								return 55;
							}
						} else {
							if (word < X53) {
								return 54;
							} else {
								return 53;
							}
						}
					} else {
						if (word < X50) {
							if (word < X51) {
								return 52;
							} else {
								return 51;
							}
						} else {
							if (word < X49) {
								return 50;
							} else {
								return 49;
							}
						}
					}
				}
			} else {
				if (word < X40) {
					if (word < X44) {
						if (word < X46) {
							if (word < X47) {
								return 48;
							} else {
								return 47;
							}
						} else {
							if (word < X45) {
								return 46;
							} else {
								return 45;
							}
						}
					} else {
						if (word < X42) {
							if (word < X43) {
								return 44;
							} else {
								return 43;
							}
						} else {
							if (word < X41) {
								return 42;
							} else {
								return 41;
							}
						}
					}
				} else {
					if (word < X36) {
						if (word < X38) {
							if (word < X39) {
								return 40;
							} else {
								return 39;
							}
						} else {
							if (word < X37) {
								return 38;
							} else {
								return 37;
							}
						}
					} else {
						if (word < X34) {
							if (word < X35) {
								return 36;
							} else {
								return 35;
							}
						} else {
							if (word < X33) {
								return 34;
							} else {
								return 33;
							}
						}
					}
				}
			}
		} else {
			if (word < X16) {
				if (word < X24) {
					if (word < X28) {
						if (word < X30) {
							if (word < X31) {
								return 32;
							} else {
								return 31;
							}
						} else {
							if (word < X29) {
								return 30;
							} else {
								return 29;
							}
						}
					} else {
						if (word < X26) {
							if (word < X27) {
								return 28;
							} else {
								return 27;
							}
						} else {
							if (word < X25) {
								return 26;
							} else {
								return 25;
							}
						}
					}
				} else {
					if (word < X20) {
						if (word < X22) {
							if (word < X23) {
								return 24;
							} else {
								return 23;
							}
						} else {
							if (word < X21) {
								return 22;
							} else {
								return 21;
							}
						}
					} else {
						if (word < X18) {
							if (word < X19) {
								return 20;
							} else {
								return 19;
							}
						} else {
							if (word < X17) {
								return 18;
							} else {
								return 17;
							}
						}
					}
				}
			} else {
				if (word < X8) {
					if (word < X12) {
						if (word < X14) {
							if (word < X15) {
								return 16;
							} else {
								return 15;
							}
						} else {
							if (word < X13) {
								return 14;
							} else {
								return 13;
							}
						}
					} else {
						if (word < X10) {
							if (word < X11) {
								return 12;
							} else {
								return 11;
							}
						} else {
							if (word < X9) {
								return 10;
							} else {
								return 9;
							}
						}
					}
				} else {
					if (word < X4) {
						if (word < X6) {
							if (word < X7) {
								return 8;
							} else {
								return 7;
							}
						} else {
							if (word < X5) {
								return 6;
							} else {
								return 5;
							}
						}
					} else {
						if (word < X2) {
							if (word < X3) {
								return 4;
							} else {
								return 3;
							}
						} else {
							if (word < X1) {
								return 2;
							} else {
								return 1;
							}
						}
					}
				}
			}
		}
	}
}
