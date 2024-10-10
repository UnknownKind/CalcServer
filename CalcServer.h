#pragma once

#include <unordered_map>
#include <set>

#include "calcelement.h"
#include "DatabaseManagements.h"
#include "LoadData.h"
#include "Logger.h"

namespace calc_server{
    
    using namespace database_managements;
    using namespace logger;
    using namespace load_data;
    using namespace calc_element;

    using DynamicLibrary = load_data::DynamicLibrary;
    using MapKKSToSetPtr = std::unordered_map<std::string, std::set<SignalInput*>>;

    using MapNameInputSignalToData =            std::unordered_map<std::string, SignalInput>;
    using MapNameCoefficientToValue =           std::unordered_map<std::string, Coefficient>;
    using MapNameTableToValueCoefficients =     std::unordered_map<std::string, MapNameCoefficientToValue>;
    using MapNameOutputSignalToVlaue =          std::unordered_map<std::string, SignalOutput>;
    using MapNameTableToValueOutputSignals =    std::unordered_map<std::string, MapNameOutputSignalToVlaue>;

    using json = nlohmann::json;
    
    static Logger logger("CalcServerLogger.txt", true);

    class CalcServer{
    public:
        CalcServer();

        ~CalcServer();

        CalcServer(const CalcServer& other) = delete;
        CalcServer& operator=(const CalcServer& other) = delete;
        CalcServer(const CalcServer&& other) = delete;
        CalcServer& operator=(const CalcServer&& other) = delete;

        void LoadDLLFunctions(const fs_path& path);
        void CreateBlocksFromJSON(const fs_path& path);
        [[nodiscard]] bool PreparingServerCalculation();
        [[nodiscard]] bool CalcOneStep(double current_time, double step_calc = 1);
        [[nodiscard]] json GenerateJSONForDebug(double time_calc, double step_calc = 1);
        void SetOutputFile(std::string name);

        [[nodiscard]] bool UpdateCoefficients(bool need_wait_update = true);
        void CheckUpdateValue(bool waiting_all = false);
        
        const std::string& GetDefaultNameInputFile(){
            return name_inp_file_json_;
        }

        void SetTimestemp(std::chrono::seconds timestemp){
            timestemp_ = timestemp;
            not_real_time_ = true;
        } 

        #ifdef DEBUG
            //Всё что находится в этой секции для отладки и в РЕЛИЗНОЙ ВЕРСИИ НЕ БУДЕТ! 
            //Если что-то из этого используется, то на свой страх и риск с последующим отключением этого функционала.
            const MapNameTableToValueOutputSignals& GetOutputSignals() const{
                return signals_output_;
            }

            MapNameInputSignalToData& GetInputSignals(){
                return signals_input_;
            }

            MapNameTableToValueCoefficients& GetCoefficients(){
                return coefficients_;
            }
                   
        #endif


    private:

        DatabaseManagements db_manager_;

        MapKKSToSetPtr update_value_;
        MapNameInputSignalToData signals_input_;
        MapNameTableToValueCoefficients coefficients_;
        MapNameTableToValueOutputSignals signals_output_;

        std::unordered_map<std::string, DynamicLibrary> upload_library_;
        std::vector<std::unique_ptr<ICalcElement>> created_blocks_;

        const SignalInput* CreateSignalInput(const json& data_signal);
        MapNameInputSignalToDataPtr LoadSignalInput(const json& input_data);
        const SignalInput* GetSignalInput(const std::string& code) const;

        SignalOutput* CreateSignalOutput(const json& data_signal);
        MapNameTableToValueOutputSignalsPtr LoadSignalOutput(const json& output_data);
        const SignalOutput* GetSignalOutput(const std::string& code, const std::string& table_name = "") const;

        MapNameTableToValueCoefficientsPtr CreateCoefficients(const json& data_signal);
        MapNameTableToValueCoefficientsPtr LoadSignalCoefficient(const json& coef_data);
        const Coefficient* GetCoefficient(const std::string& code, const std::string& table_name = "") const;

        template<typename TypeData>
        const TypeData* SearchInTable(const std::unordered_map<std::string, TypeData>& map_data, const std::string& code) const;

        using CreateFunction = std::unique_ptr<ICalcElement> (*)(MapNameInputSignalToDataPtr&& inp,
                                                        MapNameTableToValueCoefficientsPtr&& coef,
                                                        MapNameTableToValueOutputSignalsPtr&& output
        );

        [[nodiscard]] bool UpdateValueInputSignals(const std::string& name_file_inp_ = "");

        std::string name_inp_file_json_ = "ValueInputSignals.json";

        [[nodiscard]] bool ConnectDatabase(const std::string& name_connect);

        const std::string name_db_out_ = "output";
        const std::string name_db_coefficient_ = "coefficient";

        bool CheckTableExist(const std::string& name_table, const std::string& name_connection) const;
        bool UpdateListExistTable(const std::string& name_connection);
        
        bool PreparingOutputTables();
        bool PreparingRecordRequestOut();
        bool PreparingRecordRequestCoef();

        std::unordered_map<std::string, std::set<std::string>> name_db_to_exist_tables_ = {
            {name_db_out_, {}}, {name_db_coefficient_, {}}
        };

        std::unordered_map<std::string, std::string> name_table_to_request_insert_;
        std::unordered_map<std::string, std::string> name_table_to_request_select_;

        bool WriteOutputSignalsToDatabase();
        
        std::set<int> id_request_select_wait_;

        bool not_real_time_ = false;

        std::chrono::seconds timestemp_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());

    };

    template<typename TypeData>
    const TypeData* CalcServer::SearchInTable(const std::unordered_map<std::string, TypeData>& map_data, const std::string& code) const{

        auto rezult = std::find_if(map_data.begin(), map_data.end(),
            [code](const auto& item){ return item.second.code == code; });

        if(rezult == map_data.end()){
            //using ValueType = typename std::unordered_map<std::string, TypeDataPtr>::mapped_type;
            return nullptr;
        }

        return &rezult->second;
    }

    struct GetValueToStringSignalsVariantOut{
        std::string operator()(int data_int) {return std::to_string(data_int);};
        std::string operator()(double data_double) {return std::to_string(data_double);};
        std::string operator()(const std::string& data_string) {return data_string;};
        template<typename T>
        std::string operator()(T& value) {
            std::stringstream str;
            str << value;
            return str.str();
        };
    };

    #ifdef DEBUG
    //Всё что находится в этой секции для отладки и в РЕЛИЗНОЙ ВЕРСИИ НЕ БУДЕТ! 
    //Если что-то из этого используется, то на свой страх и риск с последующим отключением этого функционала.

    namespace debug_function{

        void WriteToJSONOutSignals(
            const CalcServer& server,
            const std::string& separator_before_write = "[",
            const std::string& separator_after_write = "]", 
            const std::string& name_file = "OutputValueSignals.json"
        );

        template<typename T>
        void UpdateCoefficientsFromRange(   CalcServer& server,
                                            const std::string& table_name,
                                            const std::unordered_map<std::string, T> values
                                            
        ){
            if(server.GetCoefficients().count(table_name) == 0){
                calc_server::logger.log("There is no table named \"" + table_name + "\" in the coefficients");
                return;
            }

            auto& terget_table = server.GetCoefficients()[table_name];

            for(auto& [name_coef, value] : terget_table){
                for(auto& [name_row, value] : value.data_row){
                    value = values.at(name_row);
                }
            }
                    
        } 
        
    }

    #endif

    


}//namespace calc_server
