/**
 * @file filterresultwriter.cxx
 *
 * Developer(s) of this DAQ application have yet to replace this line with a
 * brief description of the application.
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "../../appfwk/src/DAQModuleManager.hpp"
// #include "../plugins/FilterResultWriter.hpp"
#include "appfwk/ConfigurationManager.hpp" // Needed for get_dal
#include "appfwk/DAQModule.hpp"
#include "datafilter/commandline_args.hpp"
#include "datafilter/make_config_mgr.hpp"
#include "ers/ers.hpp"

using namespace dunedaq::appfwk;
using data_t = nlohmann::json;

int main(int argc, char *argv[]) {
  dunedaq::datafilter::CommandLineArgs args;
  int result = dunedaq::datafilter::parseCommandLine(argc, argv, args,
                                                     "Filter Result Writer");

  if (result != 2)
    return result;

  data_t filterresultwriter_cfg = {{"app_name", args.appName},
                                   {"sessionName", args.sessionName}};

  auto mgr1 = dunedaq::datafilter::make_config_mgr(
      args.appName, args.sessionName, args.oksConfig);

  TLOG() << "Creating Module instances for FilterResultWriter...";

  std::shared_ptr<dunedaq::appfwk::DAQModule> filterresultwriter1 =
      make_module("FilterResultWriter", "FilterResultWriter_0");
  TLOG() << "Calling init on modules...";
  filterresultwriter1->init(mgr1);
  filterresultwriter1->execute_command("conf", filterresultwriter_cfg);
  filterresultwriter1->execute_command("start", filterresultwriter_cfg);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  filterresultwriter1->execute_command("stop", filterresultwriter_cfg);

  return 0;
}
