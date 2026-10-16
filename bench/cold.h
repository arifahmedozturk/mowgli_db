#pragma once
// Page-cache helpers for the cold-start runs in bench.cpp.
#include <filesystem>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>

// Drop a closed file's pages from the OS page cache (no root needed; the file
// must not be mapped, so close its Table first).
inline void evict_from_page_cache(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    ::fdatasync(fd);
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    ::close(fd);
}

// Fraction of a file's pages currently in the page cache.
inline double resident_fraction(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return 0;
    size_t size = std::filesystem::file_size(path);
    double frac = 0;
    if (size > 0) {
        void* p = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (p != MAP_FAILED) {
            size_t page = ::sysconf(_SC_PAGESIZE), pages = (size + page - 1) / page;
            std::vector<unsigned char> vec(pages);
            if (::mincore(p, size, vec.data()) == 0) {
                size_t in = 0;
                for (unsigned char v : vec) in += v & 1;
                frac = double(in) / pages;
            }
            ::munmap(p, size);
        }
    }
    ::close(fd);
    return frac;
}

inline long major_faults() {
    rusage ru{};
    ::getrusage(RUSAGE_SELF, &ru);
    return ru.ru_majflt;
}
