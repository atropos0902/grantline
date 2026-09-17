// Package bugs holds twelve known defects behind build tags. Every constant is
// false in a normal build, so the compiler removes the branches that use it.
//
// scripts/known-bugs.sh builds the tests once per tag and expects the named
// test to fail. A test that claims to cover a rule and still passes with the
// rule broken isn't covering it.
package bugs
