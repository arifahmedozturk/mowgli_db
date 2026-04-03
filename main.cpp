#include "mql/engine.h"
#include <iostream>
#include <string>
#include <filesystem>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>

int main(int argc, char* argv[]) {
    std::string data_dir = argc > 1 ? argv[1] : "./data";
    std::filesystem::create_directories(data_dir);

    Engine engine(data_dir);

    bool interactive = isatty(STDIN_FILENO);

    if (interactive) {
        std::cout << "heavy-trie mql — data dir: " << data_dir << "\n";
        std::cout << "Type HELP for available commands. Ctrl+D or exit to quit.\n\n";
    }

    auto process = [&](const std::string& line) -> bool {
        if (line.empty()) return true;
        if (line == "exit" || line == "EXIT" || line == "quit" || line == "QUIT") return false;
        try {
            std::cout << engine.exec(line) << "\n";
        } catch (const std::exception& e) {
            std::cout << "ERROR: " << e.what() << "\n";
        }
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
        while (std::getline(std::cin, line)) {
            if (!process(line)) break;
        }
    }

    return 0;
}
