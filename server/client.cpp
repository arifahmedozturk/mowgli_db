#include "server/wire.h"
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int         port = 5432;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--host" && i + 1 < argc) { host = argv[++i]; }
        else if (a == "--port" && i + 1 < argc) { port = std::stoi(argv[++i]); }
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        // try hostname resolution
        hostent* he = ::gethostbyname(host.c_str());
        if (!he) { std::cerr << "cannot resolve host: " << host << "\n"; return 1; }
        memcpy(&addr.sin_addr, he->h_addr_list[0], sizeof(addr.sin_addr));
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("connect"); return 1;
    }

    bool interactive = isatty(STDIN_FILENO);
    if (interactive)
        std::cout << "heavy-trie  " << host << ":" << port
                  << "  — HELP for commands, exit to quit\n\n";

    auto process = [&](const std::string& line) -> bool {
        if (line.empty()) return true;
        if (line == "exit" || line == "EXIT" || line == "quit" || line == "QUIT") {
            send_msg(fd, line);
            return false;
        }
        if (!send_msg(fd, line)) {
            std::cerr << "connection lost\n";
            return false;
        }
        std::string result;
        if (!recv_msg(fd, result)) {
            std::cerr << "connection lost\n";
            return false;
        }
        std::cout << result << "\n";
        return true;
    };

    if (interactive) {
        char* raw;
        while ((raw = readline("> ")) != nullptr) {
            std::string line(raw);
            free(raw);
            if (!line.empty()) add_history(line.c_str());
            if (!process(line)) break;
        }
    } else {
        std::string line;
        while (std::getline(std::cin, line))
            if (!process(line)) break;
    }

    ::close(fd);
    return 0;
}
