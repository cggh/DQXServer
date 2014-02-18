import numpy as np
import struct


NATIVE_ENDIAN = '<' if (np.dtype("<i").byteorder == '=') else '>'

def _strict_dtype_string(dtype):
    if dtype.str[1] == 'S' or dtype.str[1] == 'U':
        return 'S'
    if not dtype.isbuiltin:
        raise Exception("Only scalar builtin dtypes (ie not structured with fields or user-defined) or strings currently supported")
#Old method commented out as length is platform dependant
#	byte_order = dtype.byteorder
#	if byte_order == '=':
#		byte_order = NATIVE_ENDIAN
#	return byte_order + dtype.char
	#New method gives explicit num bytes and endianness
    return dtype.str

#Convert a string array to a chain of null terminated strings
def pack_string_array(array):
    result = ''
    for string in np.nditer(array):
        result += str(string)
        result += chr(0)
    return result

def _encode_numpy_array(array):
    dtype = _strict_dtype_string(array.dtype)
    for char in dtype:
        yield char
    yield chr(0)
    yield struct.pack('<B', len(array.shape))
    for dim in array.shape:
        yield struct.pack('<L', dim)
    if dtype == 'S':
        data = pack_string_array(array)
    else:
        data = array.data
	yield struct.pack('<L', len(array.data))
	for byte in array.data:
		yield byte

def encode_array(array, dtype=None):
    """Encode an array for a JS arraybuffer

    array can be any iterable or a numpy array. If it is not a numpy array it will be converted to one
    with the specifed dtype.

    Returns a generator which yields bytes in the format:

    - First two bytes are 'AB'
    - A /0 terminated cstyle string which is a valid numpy dtype, but which always includes the
      endianness as first char. '<' little-endian, '>' big-endian, '|'not applicable.
    - 1-byte unsigned little endian number of dimensions = D
    - D x 4-byte unsigned little endians dimension sizes
    - 4-byte unsigned little endian buffer size (equal to the product of dimension sizes and byte length of dtype)
    - The buffer itself.

    """

    try:
        dtype = dtype or array.dtype
    except AttributeError:
        raise Exception("Non-numpy array passed, but with no numpy dtype to convert to")
    dtype = np.dtype(dtype)
    yield 'A'
    yield 'B'
    for byte in _encode_numpy_array(np.asarray(array, dtype)):
        yield byte
	
def encode_array_set(array_set):
    """Encode a set of named arrays for a set of JS arraybuffer

    array_set is an iterable of name, numpy_array tuples.

    Returns a generator which yields bytes in the format:

    - First two bytes are 'AS'
    - 1-byte unsigned little endian number of arrays
    Then for each array:
        - A /0 terminated cstyle string which contains the name of the array
        - A /0 terminated cstyle string which is a valid numpy dtype, but which always includes the
          endianness as first char. '<' little-endian, '>' big-endian, '|'not applicable.
        - 1-byte unsigned little endian number of dimensions = D
        - D x 4-byte unsigned little endians dimension sizes
        - 4-byte unsigned little endian buffer size (equal to the product of dimension sizes and byte length of dtype)
        - The buffer itself.

    """
    array_set = list(array_set)
    yield 'A'
    yield 'S'
    yield struct.pack('<B', len(array_set))
    for name, array in array_set:
        for char in name:
            yield char
        yield chr(0)
        for byte in _encode_numpy_array(array):
            yield byte

	
		
