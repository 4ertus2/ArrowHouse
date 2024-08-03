#include <IO/VarInt.h>
#include <Common/Exception.h>

namespace AH
{

void throwReadAfterEOF()
{
    throw Exception("Attempt to read after eof");
}

}
