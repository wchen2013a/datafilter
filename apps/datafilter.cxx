/**
 * @file datafilter.cxx
 *
 * Developer(s) of this DAQ application have yet to replace this line with a
 * brief description of the application.
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "../../appfwk/src/DAQModuleManager.hpp"
#include "appfwk/ConfigurationManager.hpp" // Needed for get_dal
#include "appfwk/DAQModule.hpp"
#include "datafilter/commandline_args.hpp"
#include "datafilter/dal/DataFilterConfig.hpp"
#include "datafilter/make_config_mgr.hpp"
#include "ers/ers.hpp"
#include <csignal>

using namespace dunedaq::appfwk;
using data_t = nlohmann::json;

int main(int argc, char *argv[]) {

  // dunedaq::datafilter::dal::DataFilterConfig::DataFilterConfig
  // datafilter_cfg;

  dunedaq::datafilter::CommandLineArgs args;
  int result =
      dunedaq::datafilter::parseCommandLine(argc, argv, args, "Data Filter");

  if (result != 2)
    return result;

  data_t datafilter_cfg = {{"app_name", args.appName},
                           {"sessionName", args.sessionName}};

  auto mgr1 = dunedaq::datafilter::make_config_mgr(
      args.appName, args.sessionName, args.oksConfig);

  TLOG() << "Creating Module instances for DataFilter...";
  std::shared_ptr<dunedaq::appfwk::DAQModule> datafilter1 =
      make_module("DataFilter", "DataFilter_0");
  TLOG() << "Calling init on modules... ";
  datafilter1->init(mgr1);
  datafilter1->execute_command("conf", datafilter_cfg);
  datafilter1->execute_command("start", datafilter_cfg);

  // Block until SIGINT or SIGTERM (Ctrl+C), then do a graceful stop.
  sigset_t waitset;
  sigemptyset(&waitset);
  sigaddset(&waitset, SIGINT);
  sigaddset(&waitset, SIGTERM);
  sigprocmask(SIG_BLOCK, &waitset, nullptr);
  int sig_received = 0;
  sigwait(&waitset, &sig_received);
  TLOG() << "Received signal " << sig_received << ", stopping DataFilter...";

  datafilter1->execute_command("stop", datafilter_cfg);

  return 0;
}
