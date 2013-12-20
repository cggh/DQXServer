import numpy as np
import struct


NATIVE_ENDIAN = '<' if (np.dtype("<i").byteorder == '=') else '>'

def strict_dtype_string(dtype):
	if not dtype.isnative:
		raise Exception("Only native, non composite dtypes currently supported")
	byte_order = dtype.byteorder
	if byte_order == '=':
		byte_order = NATIVE_ENDIAN
	return byte_order + dtype.char


def encode_array(array, dtype=None):
	"""Encode an array for a JS arraybuffer
	
	array can be any iterable or a numpy array. If it is not a numpy array it will be converted to one
	with the specifed dtype. 

	Returns a generator which yields bytes in the format:

	- First two bytes are 'AB'
	- A /0 terminated cstyle string which is a valid numpy dtype, but which always includes the
	  endianness as first char. '<' little-endian, '>' big-endian, '|'not applicable.
	- Four-byte unsigned little endian buffer size
	- The buffer itself.

	"""

	try:
		dtype = dtype or array.dtype
	except AttributeError:
		raise Exception("Non-numpy array passed, but with no numpy dtype to convert to")
	dtype = np.dtype(dtype)
	dtype_string = strict_dtype_string(dtype)
	array = np.asarray(array, dtype)
	
	yield 'A'
	yield 'B'
	for char in strict_dtype_string(dtype):
		yield char
	yield chr(0)
	yield struct.pack('<L', len(array.data))
	for byte in array.data:
		yield byte
	
	
	
		
