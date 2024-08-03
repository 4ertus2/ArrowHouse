#pragma once

#include <common/types.h>

/** Almost the same as x = x * exp10(exponent), but gives more accurate result.
  * Example:
  *  5 * 1e-11 = 4.9999999999999995e-11
  *  !=
  *  5e-11 = shift10(5.0, -11)
  */

double shift10(double x, int exponent);
float shift10(float x, int exponent);

long double shift10(uint64_t x, int exponent);
long double shift10(int64_t x, int exponent);
