#include "test_framework.hpp"

#include <iostream>

int main() {
    // Flush after every line so the last "[ RUN ]" survives a hard crash
    // (abort/fast-fail) when stdout is a pipe or file.
    std::cout << std::unitbuf;
    return vstest::run_all();
}
