// Files larger than 4 GB: blocks past the old 4 GB reservation must map to the
// right file offset, and blocks past the reservation must throw rather than
// MAP_FIXED over unrelated memory. Uses a sparse file, so it costs no disk.

#include "storage/buffer_pool.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

static const char* TEST_FILE = "/tmp/test_large_file.db";

int main() {
    std::remove(TEST_FILE);
    int fd = ::open(TEST_FILE, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert(fd >= 0);

    {
        BufferPool pool(0);
        const uint64_t low  = 1;
        const uint64_t high = (5ULL << 30) / BLOCK_SIZE + 3;  // ~5 GB into the file

        uint8_t* p = pool.pin_exclusive(low, fd);
        std::memset(p, 0xAB, BLOCK_SIZE);
        pool.unpin_exclusive(low);

        p = pool.pin_exclusive(high, fd);
        std::memset(p, 0xCD, BLOCK_SIZE);
        pool.unpin_exclusive(high);

        // Mapping the high block must not disturb the low one.
        const uint8_t* q = pool.pin_shared(low, fd);
        assert(q[0] == 0xAB && q[BLOCK_SIZE - 1] == 0xAB);
        pool.unpin_shared(low);

        // The high block landed at its real file offset.
        pool.flush_all(fd);
        uint8_t buf[BLOCK_SIZE];
        assert(::pread(fd, buf, BLOCK_SIZE, static_cast<off_t>(high * BLOCK_SIZE)) == BLOCK_SIZE);
        assert(buf[0] == 0xCD && buf[BLOCK_SIZE - 1] == 0xCD);

        // Past the reservation: a clean error, and the file is not grown.
        bool threw = false;
        try { pool.pin_shared((1ULL << 40) / BLOCK_SIZE, fd); }
        catch (const std::runtime_error&) { threw = true; }
        assert(threw);
        assert(::lseek(fd, 0, SEEK_END) < static_cast<off_t>(6ULL << 30));
    }

    ::close(fd);
    std::remove(TEST_FILE);
    std::puts("test_large_file: all passed");
    return 0;
}
