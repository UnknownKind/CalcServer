#include "CalcServer.h"

#include <exception>

namespace calc_server{

    std::string GetLowwerString(const std::string& str){
        std::string ans;
        for(const auto& char_str : str){
            ans += std::tolower(char_str);
        }
        return ans;
    }

    ResultInsertRequest WaitWorkThreadCheckTableNames(int iteration, int id, DatabaseManagements& db){           
        for(int i = 0; i < iteration; ++i){
            ResultInsertRequest request_ans = db.GetResultSelectFromID(id);
            if(request_ans.id_request != -1){
                return request_ans;
            }

            std::this_thread::sleep_for(0.01s);

            #ifdef DEBUG
                std::cout << "Waiting update" <<std::endl; 
            #endif

        }

        return {};           
    }

    CalcServer::CalcServer(){
        if(!ConnectDatabase(name_db_out_)){
           throw std::logic_error("Check the errors above. The connection cannot be created.");
        }

        if(!ConnectDatabase(name_db_coefficient_)){
            throw std::logic_error("Check the errors above. The connection cannot be created.");
        }
    }

    CalcServer::~CalcServer(){
        for(int i = 0; i < 3600; ++i){
            if(db_manager_.AreRequestsInProgress()){
                std::cout << "Wait" << std::endl;
                std::this_thread::sleep_for(1s);
            }else{
                break;
            }
        }

    }

    void CalcServer::CreateBlocksFromJSON(const fs_path& path){
        #ifdef DEBUG
            calc_server::logger.log("Load json files from : " + path.string());
        #endif

        size_t size_before = created_blocks_.size();

        std::vector<json> load_jsons;

        try{
            load_jsons = LoadAllFiles<json>(path, ".json");
            calc_server::logger.log("Load jsons: " +  std::to_string(load_jsons.size()), Logger::LogLevel::kInfo);
            
        }catch(const std::exception& e){
            logger.log(e.what(), Logger::LogLevel::kError);
            return;
        }

        for (const auto &load_json_model : load_jsons){
            try{
                for (const auto &elem_array_json : load_json_model){
                    std::string type_dll = "";
                    MapNameInputSignalToDataPtr signals_input_block;
                    MapNameTableToValueCoefficientsPtr coefficients_block;
                    MapNameTableToValueOutputSignalsPtr signals_output_block;

                    if (auto result_find = elem_array_json.find("Type"); result_find != elem_array_json.end()){
                        type_dll = *result_find;

                        if (upload_library_.count(type_dll) == 0){
                            calc_server::logger.log("The DLL file with the \"Type\" field: " + type_dll + " was not found", Logger::LogLevel::kError);
                            continue;
                        }
                    }else{
                        continue;
                    }

                    if (auto result_find = elem_array_json.find("Inputs"); result_find != elem_array_json.end()){
                        signals_input_block = LoadSignalInput(*result_find);
                    }else{
                        continue;
                    }

                    if (auto result_find = elem_array_json.find("Coefficients"); result_find != elem_array_json.end()){
                        coefficients_block = LoadSignalCoefficient(*result_find);
                    }else{
                        continue;
                    }

                    if (auto result_find = elem_array_json.find("Outputs"); result_find != elem_array_json.end()){
                        signals_output_block = LoadSignalOutput(*result_find);
                    }else{
                        continue;
                    }

                    created_blocks_.push_back(
                        {upload_library_.find(type_dll)->second.GetFunction<CreateFunction>("Create")(
                            std::move(signals_input_block),
                            std::move(coefficients_block),
                            std::move(signals_output_block))}
                    );

                    #ifdef DEBUG
                        calc_server::logger.log("Create blocks with type: " + type_dll);
                    #endif
                }
            }catch(const std::exception& e){
                logger.log(e.what(), Logger::LogLevel::kError);
            }
        }

        calc_server::logger.log("Created blocks: " + std::to_string(created_blocks_.size() - size_before), Logger::LogLevel::kInfo);
         
    }

    void CalcServer::LoadDLLFunctions(const fs_path& path){
        #ifdef DEBUG
            calc_server::logger.log("Load dll files from : " + path.string());
        #endif

        size_t size_before = upload_library_.size();

        for(auto& dll : LoadAllFiles<DynamicLibrary>(path, ".dll")){
            try{ 
                const std::string& type_dll = dll.GetTypeDLL();
                if(auto type_dll_find = upload_library_.find(type_dll); type_dll_find != upload_library_.end()){
                    logger.log("Two files were found : ( " + dll.GetFileName() + " and " + type_dll_find->second.GetFileName() +  ") with the type : " + type_dll, Logger::LogLevel::kWarning);
                    continue;
                }
                upload_library_.insert({type_dll, std::move(dll)});
            }catch(const std::exception& e){
                logger.log(e.what(), Logger::LogLevel::kError);
            }
        }
        
        calc_server::logger.log("Loaded DLL files: " + std::to_string(upload_library_.size() - size_before)); 
    }

    const SignalInput* CalcServer::CreateSignalInput(const json& data_signal){
        std::string code_sig = "";
        std::string kks_sig = "";

        if(auto code = data_signal.find("code"); code != data_signal.end() && (*code != "")){
            code_sig = *code;
        }else{
            throw std::logic_error("The \"Code\" field was not found. The signal cannot be created");
        }

        if(auto kks = data_signal.find("KKS"); kks != data_signal.end() && (*kks != "")){
            kks_sig = *kks;
        }else{
            kks_sig = "KKS_" + code_sig;
            logger.log("The \"KKS\" field was not found. Assigned the values \"KKS_\" + Code: " + kks_sig, Logger::LogLevel::kDebug);
        }

        SignalInput* sig_inp = &signals_input_[code_sig];

        if(sig_inp->code == ""){

            sig_inp->code = std::move(code_sig);
            sig_inp->kks = std::move(kks_sig);

            if(auto type = data_signal.find("type"); type != data_signal.end() && (*type != "")){
                sig_inp->type = static_cast<SignalInput::TypeSignals>((*type).dump()[1]);
            }else{
                throw std::logic_error("Invalid value of the \"Type\" field");
            }    

            if(sig_inp->type == SignalInput::TypeSignals::kTS_int){
                sig_inp->value = 0;
            }else if(sig_inp->type == SignalInput::TypeSignals::kTS_double){
                sig_inp->value = 0.0;
            }else if(sig_inp->type == SignalInput::TypeSignals::kTS_string){
                sig_inp->value = "";
            }
        }

        update_value_[sig_inp->kks].insert(sig_inp);

        return sig_inp;
    }

    const SignalOutput* CalcServer::GetSignalOutput(const std::string& code, const std::string& table_name) const{

        if(table_name == ""){
            for(const auto& [current_table_name, current_table_content]: signals_output_){
                auto result = SearchInTable(current_table_content, code);
                if(result != nullptr){
                    return result;
                }
            }
        }else{
            auto iterator_pointing_to_table = signals_output_.find(table_name);

            if(iterator_pointing_to_table != signals_output_.end()){
                return SearchInTable(iterator_pointing_to_table->second, code);
            }
        }
        
        return nullptr;
    }
    
    const Coefficient* CalcServer::GetCoefficient(const std::string& code, const std::string& table_name) const{

        if(table_name == ""){
            for(const auto& [current_table_name, current_table_content]: coefficients_){
                auto result = SearchInTable(current_table_content, code);
                if(result != nullptr){
                    return result;
                }
            }
        }else{
            auto iterator_pointing_to_table = coefficients_.find(table_name);

            if(iterator_pointing_to_table != coefficients_.end()){
                return SearchInTable(iterator_pointing_to_table->second, code);
            }
        }

        return nullptr; 
    }

    const SignalInput* CalcServer::GetSignalInput(const std::string& code) const{
        if(signals_input_.count(code) == 0){
            return nullptr;
        }

        return &signals_input_.at(code);
    }

    MapNameTableToValueCoefficientsPtr CalcServer::CreateCoefficients(const json& data_signal){
        MapNameTableToValueCoefficientsPtr coefficients;
        std::string table_name;

        if(auto iter_table_name = data_signal.find("table_name"); iter_table_name == data_signal.end()){
            throw std::logic_error("Empty \"Coefficients\" object was found");
        }else{
            table_name = *iter_table_name;
        }

        auto iter_code_signals = data_signal.find("code_signals");
        if(data_signal.find("code_signals") == data_signal.end()){
            throw std::logic_error("Empty \"Coefficients\" object was found");
        }

        for(const auto& code_and_row_data : *iter_code_signals){
            auto code = code_and_row_data.find("code");

            if( code == code_and_row_data.end()){
                throw std::logic_error("Empty \"code\" field when parsing " + table_name);
            }

            Coefficient* coef_for_insert = &coefficients_[table_name][*code];
            coef_for_insert->code = *code;

            for(const auto& row_data : *code_and_row_data.find("row")){
                coef_for_insert->data_row[row_data];
            }

            coefficients[table_name][*code] = coef_for_insert;
        }

        return coefficients;
    }

    SignalOutput* CalcServer::CreateSignalOutput(const json& data_signal){
        std::string code_str;
        std::string table_name_str;
        std::string table_col_str;

        if(auto code = data_signal.find("code"); code == data_signal.end()){
            throw std::logic_error("The \"Code\" field was not found in the output signal");
        }else{
            code_str = *code;
        }

        if(auto col_name = data_signal.find("table_col"); col_name == data_signal.end()){
            throw std::logic_error("The \"col_name\" field was not found in the output signal");
        }else{
            table_col_str = *col_name;
        }

        if(auto table_name = data_signal.find("table_name"); table_name == data_signal.end()){
            throw std::logic_error("The \"table_name\" field was not found in the output signal");
        }else{
            table_name_str = *table_name;
        }

        SignalOutput* sig_out = &signals_output_[table_name_str][code_str];

        if(sig_out->code == ""){
            sig_out->code = std::move(code_str);
            sig_out->col_name = std::move(table_col_str);
            sig_out->table_name = std::move(table_name_str);
        }

        return sig_out;
    }

    MapNameInputSignalToDataPtr CalcServer::LoadSignalInput(const json& input_data){
        MapNameInputSignalToDataPtr signals_input;

        for(const auto& input : input_data){
            auto* sig_input = CreateSignalInput(input);
            signals_input.insert({sig_input->code, sig_input});
        }
        
        return signals_input;
    }

    MapNameTableToValueOutputSignalsPtr CalcServer::LoadSignalOutput(const json& output_data){
        MapNameTableToValueOutputSignalsPtr signals_output;

        for(const auto& output : output_data){
            auto* output_signal = CreateSignalOutput(output);

            std::string table_name = output_signal->table_name;
            std::string code = output_signal->code;

            signals_output[std::move(table_name)].insert({std::move(code), output_signal});
        }

        return signals_output;
    }

    MapNameTableToValueCoefficientsPtr CalcServer::LoadSignalCoefficient(const json& coef_data){
        MapNameTableToValueCoefficientsPtr coefficients;

        for(const auto& coef : coef_data){
            coefficients.merge(CreateCoefficients(coef));               
        }
        
        return coefficients;
    }

    bool CalcServer::CalcOneStep(double current_time, double step_calc){

        if(!UpdateValueInputSignals()){
            return false; 
        }

        try{
            for(const auto& block: created_blocks_){
                block->Process(current_time, step_calc);
            }

            if(not_real_time_){
                ++timestemp_;
            }else{
                timestemp_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
            }

            return WriteOutputSignalsToDatabase();
            
        }catch(const std::exception& e){
            calc_server::logger.log(e.what(), Logger::LogLevel::kError);
            std::cerr << e.what() << '\n';

            return false;
        }
    }

    json CalcServer::GenerateJSONForDebug(double time_calc, double step_calc){

        json json_with_input_sig;

        for(const auto& function_dll : created_blocks_){
            function_dll->GenerateDebugData(json_with_input_sig, time_calc, step_calc);
        }

        return json_with_input_sig;
    }

    bool CalcServer::UpdateValueInputSignals(const std::string& name_file_inp){
        std::ifstream in;

        const std::string& name_file = (name_file_inp == "") ?  name_inp_file_json_ : name_file_inp;

        in.open(name_file);
        if(!in.is_open()){
            calc_server::logger.log("It is not possible to open a file with the name: " + name_file, Logger::LogLevel::kCritical);

            std::cerr << "It is not possible to open a file with the name: " << name_file << std::endl;

            return false;
        }

        json dataIn;
        dataIn = json::parse(in);

        for(auto& [kks, set_value] : update_value_){
            auto TargItem = dataIn.find(kks);
            if(TargItem != dataIn.end()){
                for(auto& input_sig_ptr : set_value){
                    if(TargItem->is_number_integer()) {
                        input_sig_ptr->value = static_cast<int>(TargItem.value());
                    }else if(TargItem->is_number_float()) {
                        input_sig_ptr->value = static_cast<double>(TargItem.value());
                    }else if(TargItem->is_string()) {
                        input_sig_ptr->value = static_cast<std::string>(TargItem.value());
                    }
                }

            }else if(kks.size() < 8 && kks[0] == 'K' && kks[1] == 'K' && kks[2] == 'S' && kks[3] == '_' && std::isdigit(kks[4]) && std::isdigit(kks[5]) && std::isdigit(kks[6])){
                calc_server::logger.log("It is impossible to find a signal from KKS: " + kks, Logger::LogLevel::kError);
            }
        }

        in.close();

        return true;
    }

    void CalcServer::SetOutputFile(std::string name){
        name_inp_file_json_ = std::move(name);
    }

    [[nodiscard]] bool CalcServer::ConnectDatabase(const std::string& name_connect){

            using namespace std::literals;
            std::ifstream json_model_file("ConfigDB.json");

            if(!json_model_file.is_open()){
                logger.log("The JSON file with the settings was not found: "s + "ConfigDB.json"s, Logger::LogLevel::kError);
                return false;
            }
            
            const json config_db = json::parse(json_model_file);
            auto name_config = config_db.find(name_connect);

            if(name_config == config_db.end()){
                logger.log("No data was found for the configuration launch of the database. Name configuration: " + name_connect, Logger::LogLevel::kError);
                return false;
            }

            auto data_connection = ConnectionInfo::ConnectionInfo::ConvertFromJSON(
                *name_config
            );

            data_connection.id_connection = name_connect;

            if(data_connection.dbname == ""){
                logger.log(R"(The "DataBaseName" field was not found for the configuration )" + name_connect, Logger::LogLevel::kError);
                return false;
            }

            if(data_connection.hostname == ""){
                logger.log(R"(The "HostName" field was not found for the configuration )" + name_connect, Logger::LogLevel::kError);
                return false;
            }

            if(data_connection.user == ""){
                logger.log(R"(The "UserName" field was not found for the configuration )" + name_connect, Logger::LogLevel::kError);
                return false;
            }

            if(data_connection.password == ""){
                logger.log(R"(The "Password" field was not found for the configuration )" + name_connect, Logger::LogLevel::kError);
                return false;
            }

            if(!db_manager_.CreateConnectionDatabase(data_connection)){
                logger.log( R"(Не удалось создать подключение к базе данных с настройками, указанными в "ConfigDB.json" для конфигурации: )" + name_connect, Logger::LogLevel::kCritical);
                return false;
            }

            return true;
    }

    bool CalcServer::PreparingServerCalculation(){

        if(created_blocks_.empty()){
            logger.log("There are no calculation blocks created. The necessary configuration data may not have loaded. Check the errors above.", Logger::LogLevel::kCritical);
            return false;
        }

        UpdateListExistTable(name_db_coefficient_);

        for(const auto& [name_table, content] : coefficients_){
            if(!CheckTableExist(name_table, name_db_coefficient_)){
                logger.log("Coefficients. The table does not exist in the database specified for connection: " + name_table, Logger::LogLevel::kCritical);
                return false;
            }
        }

        return PreparingOutputTables() && PreparingRecordRequestCoef();
    }


    bool CalcServer::PreparingOutputTables(){

        UpdateListExistTable(name_db_out_);

        for(const auto& [name_table, content] : signals_output_){
            if(!CheckTableExist(name_table, name_db_out_)){
                std::string query_create = "CREATE TABLE " + GetLowwerString(name_table) + " (" + "id bigserial, ";

                if(name_table.back() != '9'){
                    query_create += "timestemp character varying, ";
                }
                
                bool first = true;
                for (const auto& [name_sig, data_out] : content){
                    
                    if(first){
                        query_create += data_out.col_name + " character varying";
                        first = false;
                        continue;
                    }

                    query_create += ", " + data_out.col_name + " character varying";
                }
                
                (name_table.back() == '6') ? query_create += ", status BOOLEAN NOT NULL DEFAULT false": query_create;
                query_create += ")";
                std::string id_connection = name_db_out_;

                auto ans_insert_queue = db_manager_.InsertRequestInQueue(
                                                        id_connection.data(),
                                                        query_create.data(),
                                                        {},
                                                        Command::kInsert,
                                                        name_table
                                                    );

                if(ans_insert_queue.result_request != ResultRequest::kInProcessing){
                    return false;
                }

                while(db_manager_.AreRequestsInProgress()){std::this_thread::sleep_for(0.1s);}
                std::this_thread::sleep_for(0.1s);

                logger.log("A table with the name has been created: " + name_table, Logger::LogLevel::kInfo);

            }else{
                std::string query_select = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + name_table + "' AND table_schema = 'public'";

                auto ans_insert_queue = db_manager_.InsertRequestInQueue(
                                                    name_db_out_.data(),
                                                    query_select.data(),
                                                    {},
                                                    Command::kSelect,
                                                    name_table
                                                );

                if(ans_insert_queue.result_request != ResultRequest::kInProcessing){
                    logger.log("Check the \"DatabaseLog.txt\" file for more information", Logger::LogLevel::kCritical);
                    return false;
                }

                while(db_manager_.AreRequestsInProgress()){std::this_thread::sleep_for(0.1s);}

                auto exist_column = WaitWorkThreadCheckTableNames(11, ans_insert_queue.id_request, db_manager_);
                
                if(exist_column.id_request == -1){
                    return false;
                }

                std::string quere_request_new_column;
                std::string type_value = " character varying";
                bool first = true;

                auto& map_column = exist_column.code_to_map_field_value;

                for(const auto& output_signal : content){
                    if(map_column.find(GetLowwerString(output_signal.second.col_name)) == map_column.end()){
                        if(first){
                            quere_request_new_column = " ADD COLUMN " + output_signal.second.col_name + type_value;
                            first = false;
                            continue;
                        }
                        quere_request_new_column += ", ADD COLUMN " + output_signal.second.col_name + type_value;
                    }
                }

                std::string quere = "ALTER TABLE " + name_table;
                (!map_column.contains(("id"))) ?
                            (quere_request_new_column == "") ? quere_request_new_column += " ADD COLUMN id bigserial" : quere_request_new_column += ", ADD COLUMN id bigserial" :
                            quere_request_new_column ;

                (!map_column.contains(("timestemp"))) ?
                            (quere_request_new_column == "") ? quere_request_new_column += " ADD COLUMN timestemp character varying" : quere_request_new_column += ", ADD COLUMN timestemp character varying" :
                            quere_request_new_column ;

                (name_table.back() == '6' && (!map_column.contains(("status")))) ?
                            (quere_request_new_column != "") ?
                                quere_request_new_column += ", ADD COLUMN status BOOLEAN NOT NULL DEFAULT false":
                                quere_request_new_column += " ADD COLUMN status BOOLEAN NOT NULL DEFAULT false":
                            quere_request_new_column;
                quere += quere_request_new_column;

                if(quere != "ALTER TABLE " + name_table){
                    std::string id_connection = name_db_out_;

                    auto ans_insert_queue = db_manager_.InsertRequestInQueue(
                                                            id_connection.data(),
                                                            quere.data(),
                                                            {},
                                                            Command::kInsert,
                                                            name_table
                                                        );

                    if(ans_insert_queue.result_request != ResultRequest::kInProcessing){
                        logger.log("Request processing error : " + quere + " in table: " + name_table, Logger::LogLevel::kCritical);
                        return false;
                    }

                    while(db_manager_.AreRequestsInProgress()){std::this_thread::sleep_for(0.1s);}
                    std::this_thread::sleep_for(0.2s);

                    logger.log("Successful request processing: " + quere_request_new_column + " in table: " + name_table, Logger::LogLevel::kInfo);
                }
            }
        }
        
        return PreparingRecordRequestOut();
    }

    bool CalcServer::PreparingRecordRequestOut(){

        for(const auto& [table_name, data_table] : signals_output_){
            bool first = true;
            std::string quere_request_column = "INSERT INTO " + GetLowwerString(table_name) + " (";;
            std::string quere_request_placeholder = "(";
            size_t num_place = 1;

            for(const auto& [name_signal, value_signal] : data_table){
                if(name_signal == "timestemp"){
                    continue;
                }

                if(first){
                    quere_request_column += value_signal.col_name;
                    quere_request_placeholder += "$" + std::to_string(num_place++);
                    first = false;
                    continue;
                }

                quere_request_column += ", " + value_signal.col_name;
                quere_request_placeholder += ", $" + std::to_string(num_place++);
            }

            quere_request_column += ", " + std::string{"timestemp"};
            quere_request_placeholder += ", $" + std::to_string(num_place++);

            quere_request_column += ")";
            quere_request_placeholder += ")";

            name_table_to_request_insert_[table_name] = {std::move(quere_request_column) + " VALUES " + std::move(quere_request_placeholder)};

            #ifdef DEBUG
                logger.log("Request insert: " + name_table_to_request_insert_[table_name] + " in table: " + table_name);
            #endif
        }           

        return true;

    }

    bool CalcServer::WriteOutputSignalsToDatabase(){
        
        for(const auto& [table_name, data_table] : signals_output_){

            bool no_empty_data = false;
            std::vector<std::string> value_signals;
            value_signals.reserve(data_table.size());

            for(const auto& [name_signal, value_signal] : data_table){

                if(name_signal == "timestemp"){
                    continue;
                }                

                std::string write_value = std::to_string(static_cast<int>(value_signal.highlight_value.current_color)) + " ";
                write_value += std::visit(GetValueToStringSignalsVariantOut(), value_signal.value);
                if(value_signal.id_problem != "" && true){
                    write_value += " ";
                    write_value += value_signal.id_problem;
                }

                if(!no_empty_data && (write_value != "0 ") && (write_value != "0  ")){
                    no_empty_data = true;
                }

                value_signals.emplace_back(std::move(write_value));
            }

            value_signals.emplace_back(std::move(std::to_string(timestemp_.count())));
        

            if(no_empty_data){
                auto result_insert = db_manager_.InsertRequestInQueue(
                                            name_db_out_.data(),
                                            name_table_to_request_insert_[table_name].data(),
                                            std::move(value_signals),
                                            Command::kInsert,
                                            table_name
                                        );

                if(result_insert.result_request != ResultRequest::kInProcessing){
                    logger.log("It is not possible to insert into the database " + table_name + "Code error: " + std::to_string(static_cast<int>(result_insert.result_request)), Logger::LogLevel::kCritical);
                    return false;
                }
            }

        }

        return true;
    }

    bool CalcServer::PreparingRecordRequestCoef(){

        for(auto& [table_name, data_table] : coefficients_){
            name_table_to_request_select_[table_name] = "SELECT * FROM " + GetLowwerString(table_name) ;
        } 

        return UpdateCoefficients(true);
    }

    [[nodiscard]] bool CalcServer::UpdateCoefficients(bool need_wait_update){
        
        for(auto& [table_name, data_table] : coefficients_){

            auto result_select = db_manager_.InsertRequestInQueue(
                                                    name_db_coefficient_.data(),
                                                    name_table_to_request_select_[table_name].data(),
                                                    {},
                                                    Command::kSelect,
                                                    table_name
                                            );

            if(result_select.result_request != ResultRequest::kInProcessing){
                logger.log("Check the \"DatabaseLog.txt\" file for more information", Logger::LogLevel::kCritical);
                return false;
            }

            id_request_select_wait_.insert(result_select.id_request);
        } 

        CheckUpdateValue(need_wait_update);

        return true;

    }

    void CalcServer::CheckUpdateValue(bool waiting_all){
        
        if(waiting_all){
            while(db_manager_.AreRequestsInProgress()){
                std::this_thread::sleep_for(0.1s);
            }
        }

        //#ifdef DEBUG
            auto start = std::chrono::system_clock::now();
        //#endif

        do{
            std::vector<int> remove_id;

            for(int id : id_request_select_wait_){
                auto exist_table = WaitWorkThreadCheckTableNames(50, id, db_manager_);
            
                if(exist_table.id_request != -1){
                    #ifdef DEBUG
                        auto start_app = std::chrono::system_clock::now();
                    #endif

                    for(auto& [name_signal, data_signal] : coefficients_[std::string(exist_table.name_table)]){
                        for(auto& [name_row, value_row] : data_signal.data_row){
                            std::string& str = exist_table.code_to_map_field_value.at(name_signal).at(name_row);
                            char* end;

                            // Попробуем преобразовать в число с плавающей точкой
                            double d = std::strtod(str.c_str(), &end);
                            (*end == '\0') ? value_row = d : value_row = str;

                        }
                    }

                    remove_id.push_back(id);
                    #ifdef DEBUG
                        std::cout << "Update coef table: " << exist_table.name_table <<std::endl;
                        std::cout << "Update time: "<< std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start_app).count() <<std::endl;
                    #endif
                }
            }

            if(db_manager_.GetSizeQueueSelect() == 0){
                id_request_select_wait_.clear();
            }

            for(int id_remove : remove_id){
                id_request_select_wait_.erase(id_remove);
            }

        }while(waiting_all && !id_request_select_wait_.empty());

        //#ifdef DEBUG
            std::cout << "Update coef total: "<< std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start).count() <<std::endl;
        //#endif
    }

    bool CalcServer::CheckTableExist(const std::string& name_table, const std::string& name_connection) const{
        return (name_db_to_exist_tables_.at(name_connection).count(name_table) > 0 || name_db_to_exist_tables_.at(name_connection).count(GetLowwerString(name_table)) > 0);
    }

    bool CalcServer::UpdateListExistTable(const std::string& name_connection){
            std::string query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'";

            std::string id_connection = name_connection;

            auto ans_insert_queue = db_manager_.InsertRequestInQueue(
                                                    id_connection.data(),
                                                    query.data(),
                                                    {},
                                                    Command::kSelect,
                                                    "All_table"
                                                );

            if(ans_insert_queue.result_request != ResultRequest::kInProcessing){
                logger.log("Check the \"DatabaseLog.txt\" file for more information", Logger::LogLevel::kCritical);
                return false;
            }

            while(db_manager_.AreRequestsInProgress()){std::this_thread::sleep_for(0.1s);}

            auto exist_table = WaitWorkThreadCheckTableNames(11, ans_insert_queue.id_request, db_manager_);
            
            if(exist_table.id_request == -1){
                return false;
            }
            
            name_db_to_exist_tables_[name_connection].clear();
            for(auto& [name_row, data_value] : exist_table.code_to_map_field_value){
                name_db_to_exist_tables_[name_connection].insert(name_row); 
            }

            return true;
    }

    #ifdef DEBUG
    //Всё что находится в этой секции для отладки и в РЕЛИЗНОЙ ВЕРСИИ НЕ БУДЕТ! 
    //Если что-то из этого используется, то на свой страх и риск с последующим отключением этого функционала.
    namespace debug_function{

        void WriteToJSONOutSignals( const CalcServer& server,
                                    const std::string& separator_before_write,
                                    const std::string& separator_after_write,
                                    const std::string& name_file
            ){
                static std::ofstream json_file_with_output(name_file);

                if (!json_file_with_output.is_open()) {
                    calc_server::logger.log("It is not possible to create a json_file to record the otput results");

                    return;
                }

                json json_array;
                json_file_with_output << separator_before_write << "\n";

                for(const auto& [name_table, content_table] : server.GetOutputSignals()){
                    for(const auto& [name_signals, value_signals] : content_table){
                        json_array.push_back(json::object({
                                                            {"code", value_signals.code},
                                                            {"col_name", value_signals.col_name},
                                                            {"table_name", value_signals.table_name},
                                                            {"value", std::visit(GetValueToStringSignalsVariantOut(), value_signals.value)}
                                                        })
                                            );

                    }
                }

                json_file_with_output << json_array;
                json_file_with_output << separator_after_write << "\n";
        }

    }
    #endif
}//namespace calc_server