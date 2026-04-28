#include <sys/sysinfo.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>  // for std::pair
#include <vector>

namespace dunedaq {
namespace datafilter {

using namespace std;
struct node_info {
    vector<pair<string, string>> get_node_info() {
        vector<pair<string, string>> node_info;

        // Get OS information using uname
        struct utsname uts;
        if (uname(&uts) == 0) {
            node_info.push_back({"OS Name", uts.sysname});
            node_info.push_back({"Hostname", uts.nodename});
            node_info.push_back({"Hardware Architechture", uts.machine});
            node_info.push_back({"Kernel Version", uts.version});
        } else {
            cerr << "Failed to get OS information using uname." << endl;
        }

        // Get uptime and other sysinfo
        struct sysinfo si;
        if (sysinfo(&si) == 0) {
            long uptime = si.uptime;
            int days = uptime / (3600 * 24);
            int hours = (uptime % (3600 * 24)) / 3600;
            int minutes = (uptime % 3600) / 60;
            int seconds = uptime % 60;
            ostringstream oss;
            oss << days << " days, " << hours << ":" << minutes << ":"
                << seconds;
            node_info.push_back({"Uptime", oss.str()});
            node_info.push_back(
                {"NPROCESSORS", std::to_string(sysconf(_SC_NPROCESSORS_ONLN))});
            double loadavg[3];
            if (getloadavg(loadavg, 3) > 0) {
                oss.str("");
                oss.clear();
                oss << fixed << setprecision(2);
                oss << loadavg[0] << " " << loadavg[1] << " " << loadavg[2];
                node_info.push_back({"Load Average", oss.str()});
            } else {
                cerr << "Failed to get load average." << endl;
            }
        } else {
            cerr << "Failed to get system information using sysinfo." << endl;
        }

        return node_info;
    }
};
}  // namespace datafilter
}  // namespace dunedaq
