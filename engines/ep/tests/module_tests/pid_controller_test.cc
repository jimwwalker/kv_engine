#include "pid_controller.h"

#include <folly/portability/GTest.h>

struct myclock {
    static std::chrono::milliseconds currentTime;
    static std::chrono::time_point<std::chrono::steady_clock> now() {
        currentTime += std::chrono::milliseconds{1000};
        std::chrono::steady_clock::duration s = currentTime;
        return std::chrono::time_point<std::chrono::steady_clock>{s};
    }
};

std::chrono::milliseconds myclock::currentTime{0};

TEST(PidController, JWW) {
    PIDController pid{
            12.0, 0.008, 0.00000001, 1.0, std::chrono::milliseconds{10000}};

    // STEP
    float frag = 12;
    for (int i = 0; i < 1000; i++) {
        frag += 0.002;
        if (frag >= 100) {
            frag = 100;
        }
        pid.step<myclock>(frag);
        std::cerr << " "
                  << std::chrono::duration_cast<std::chrono::seconds>(
                             myclock::currentTime)
                             .count()
                  << "s frag:" << frag << " " << pid << std::endl;
    }
    for (int i = 0; i < 1000; i++) {
        pid.step<myclock>(frag);
        std::cerr << " "
                  << std::chrono::duration_cast<std::chrono::seconds>(
                             myclock::currentTime)
                             .count()
                  << "s frag:" << frag << " " << pid << std::endl;
    }
    for (int i = 0; i < 1000; i++) {
        frag -= 0.002;
        pid.step<myclock>(frag);
        std::cerr << " "
                  << std::chrono::duration_cast<std::chrono::seconds>(
                             myclock::currentTime)
                             .count()
                  << "s frag:" << frag << " " << pid << std::endl;
    }
}