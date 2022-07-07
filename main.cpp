#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <vector>
#include <cstring>

#define MAXBUFSIZE 4096

typedef enum command_type{
    PROCESS_BUNDLE_CREATE,      // Indicate that pbc command is received.
    PROCESS_BUNDLE_STOP,        // Indicate that pbs command is received.
    PROCESS_BUNDLE_EXECUTION,   // Indicate that one or more bundles want to be executed.
    QUIT                        // Indicate that quit command is received.
}command_type;

typedef struct bundle_execution {
    char *name;     // Name of the bundle that is meant to be executed.
    char *output;   // Output redirection file name of the bundle.
    char *input;    // Input redirection file name of the bundle.
} bundle_execution;

typedef struct parsed_command {
    command_type type; // To show which command it is.
    char *bundle_name; // Name of the bundle if the command is pbc.
    int bundle_count;  // If there is a bundle execution, this indicates the number of bundles in the pipeline.

    bundle_execution *bundles;

} parsed_command;

typedef union parsed_input {
    parsed_command command; // This is filled correctly when the input is a specific command
    char **argv;     // This is filled when the input is a process inside a bundle
} parsed_input;

int parse(char *line, int is_bundle_creation, parsed_input *parsedInput) {
    if ( !parsedInput )
        return 0;
    int is_quoted;
    char buffer[256];
    int argument_count, argument_index;
    int index;

    memset(buffer, 0, sizeof(char)*256);
    if ( is_bundle_creation ) {
        is_quoted = 0;
        index = 0;
        argument_index = 0;
        argument_count = 0;
        for ( char *current = line; *current ; current++) {
            if ( is_quoted ) {
                if ( *current == '"' ) {
                    is_quoted = 0;
                }
                else
                    index++;
            }
            else {
                if (*current == '"')
                    is_quoted = 1;
                else if (isspace(*current)) {
                    if ( index == 0 ) continue;
                    argument_count++;
                    index = 0;
                }
                else
                    index++;
            }
        }

        is_quoted = 0;
        index = 0;
        for ( char *current = line; *current ; current++) {
            if ( is_quoted ) {
                if ( *current == '"' )
                    is_quoted = 0;
                else
                    buffer[index++] = *current;
            }
            else {
                if ( *current == '"' )
                    is_quoted = 1;
                else if ( isspace(*current) ) {
                    if ( index == 0 ) continue;
                    else if ( strcmp(buffer, "pbs") == 0 ) {
                        parsedInput->command.type = PROCESS_BUNDLE_STOP;
                        return 1;
                    }
                    buffer[index++] = '\0';
                    if ( argument_index == 0 ) {
                        parsedInput->argv = (char **)calloc(argument_count+1, sizeof(char*));
                        parsedInput->argv[argument_count] = NULL;
                    }
                    parsedInput->argv[argument_index] = (char*) calloc(index, sizeof(char));
                    strcpy(parsedInput->argv[argument_index], buffer);
                    index = 0;
                    argument_index++;
                }
                else
                    buffer[index++] = *current;
            }
        }
    }
    else {
        is_quoted = 0;
        argument_count = 1;
        for ( char *current = line; *current ; current++) {
            if (is_quoted) {
                if (*current == '"')
                    is_quoted = 0;
            }
            else {
                if (*current == '"')
                    is_quoted = 1;
                else if ( *current == '|' )
                    argument_count++;
            }
        }
        is_quoted = 0;
        index = 0;
        argument_index = 0;
        int is_next_input, is_next_output, executable_index;
        for ( char *current = line; *current ; current++) {
            if (is_quoted) {
                if (*current == '"')
                    is_quoted = 0;
                else
                    buffer[index++] = *current;
            } else {
                if (*current == '"')
                    is_quoted = 1;
                else if (isspace(*current)) {
                    if ( index == 0 ) continue;
                    buffer[index++] = '\0';
                    if (!argument_index) {
                        if ( strcmp(buffer, "pbc") == 0 )
                            parsedInput->command.type = PROCESS_BUNDLE_CREATE;
                        else if ( strcmp(buffer, "pbs") == 0 ) {
                            parsedInput->command.type = PROCESS_BUNDLE_STOP;
                            return 1;
                        }
                        else if ( strcmp(buffer, "quit") == 0 ) {
                            parsedInput->command.type = QUIT;
                            return 0;
                        }
                        else {
                            parsedInput->command.type = PROCESS_BUNDLE_EXECUTION;
                            parsedInput->command.bundle_count = argument_count;
                            parsedInput->command.bundles = (bundle_execution*) calloc(argument_count, sizeof(bundle_execution));

                            is_next_input = 0;
                            is_next_output = 0;
                            executable_index = 0;

                            parsedInput->command.bundles[executable_index].name = (char*)calloc(index, sizeof(char));
                            strcpy(parsedInput->command.bundles[executable_index].name, buffer);

                            parsedInput->command.bundles[executable_index].input = NULL;
                            parsedInput->command.bundles[executable_index].output = NULL;

                        }
                    }
                    else {
                        if ( parsedInput->command.type == PROCESS_BUNDLE_CREATE ) {
                            parsedInput->command.bundle_name = (char*) calloc(index, sizeof(char));
                            strcpy(parsedInput->command.bundle_name, buffer);
                            return 0;
                        }
                        else {
                            if ( argument_index % 2 == 1 ) {
                                if ( strcmp(buffer, "<") == 0 )
                                    is_next_input = 1;
                                else if ( strcmp(buffer, ">") == 0 )
                                    is_next_output = 1;
                                else if ( strcmp(buffer, "|") == 0 )
                                    executable_index++;
                            }
                            else {
                                if ( is_next_input ) {
                                    parsedInput->command.bundles[executable_index].input = (char*)calloc(index, sizeof(char));
                                    strcpy(parsedInput->command.bundles[executable_index].input, buffer);
                                    is_next_input = 0;
                                }
                                else if ( is_next_output ) {
                                    parsedInput->command.bundles[executable_index].output = (char*)calloc(index, sizeof(char));
                                    strcpy(parsedInput->command.bundles[executable_index].output, buffer);
                                    is_next_output = 0;
                                }
                                else {
                                    parsedInput->command.bundles[executable_index].name = (char*)calloc(index, sizeof(char));
                                    strcpy(parsedInput->command.bundles[executable_index].name, buffer);
                                }
                            }
                        }
                    }
                    index = 0;
                    argument_index++;
                } else
                    buffer[index++] = *current;
            }
        }
    }
    return 0;
}

class operationsClass {
private:
    char* pathName;
    std::vector<char*> argv;
public:
    char* getPathName() {
        return this->pathName;
    }
    std::vector<char*> getArgv() {
        return this->argv;
    }
    operationsClass(char* pathName) {
        this->pathName = pathName;
    }
    void addArgv(char* argv) {
        this->argv.push_back(argv);
    }
};

class bundleClass {
private:
    std::string  bundleName;
    std::vector<operationsClass> operations;
public :
    std::string getName() {
        return this->bundleName;
    }
    std::vector<operationsClass> getOperations() {
        return this->operations;
    }

    bundleClass() {}
    bundleClass(std::string bundleName) {
        this->bundleName = bundleName;
    }

    void addOperations(char* pathName, char** argv) {
        operationsClass operation = operationsClass(pathName);
        operation.addArgv(pathName);
        for(;*argv; *argv++){
            operation.addArgv(*argv);
        }
        operation.addArgv(NULL);
        this->operations.push_back(operation);
    }
};

int main() {
    char buffer[256];
    int bufferSize, is_bundle_creation = 0;
    parsed_input parsedInput;
    std::vector<bundleClass> bundleVector;
    while(1) {
        std::cin.getline(buffer, 256);
        bufferSize = strlen(buffer);
        buffer[bufferSize] = '\n';
        buffer[bufferSize + 1] = '\0';
        parse(buffer, is_bundle_creation, &parsedInput);
        if(parsedInput.command.type == QUIT) {
            break;
        }
        if(is_bundle_creation) {
            if(parsedInput.command.type == PROCESS_BUNDLE_STOP) {
                is_bundle_creation = false;
            } else {
                char** argv = parsedInput.argv;
                bundleVector.at(0).addOperations(*argv, argv+1);
            }
        } else {
            if(parsedInput.command.type == PROCESS_BUNDLE_CREATE) {
                bundleVector.insert(bundleVector.begin(),bundleClass(parsedInput.command.bundle_name));
                is_bundle_creation = true;
            } else if (parsedInput.command.type == PROCESS_BUNDLE_EXECUTION) {
                pid_t master;
                int bundleCount = parsedInput.command.bundle_count;
                if((master = fork()) == 0) {
                    int input, output;
                    int bundleNo;
                    if (bundleCount != 1) {
                        pid_t pid_for_bundler = getpid();
                        bundleNo = 0;
                        int fd[bundleCount-1][2];
                        for(int i = 0; i < bundleCount-1; i++) {
                            //std::cout << "pipeNo: " << i << std::endl;
                            pipe(fd[i]);
                        }
                        for (int i = 0; bundleNo < bundleCount; i++){
                            if(fork()){
                                bundleNo++;
                            } else {
                                //std::cout << "child: " << bundleNo << std::endl;
                                break;
                            }
                        }
                        if(pid_for_bundler == getpid()){
                            //std::cout << "bundler bekliyor" << std::endl;
                            //std::cout << "BUNDLER GİRDİ." << std::endl;
                            for(int i = 0; i < bundleCount-1; i++){
                                close(fd[i][0]);
                                close(fd[i][1]);
                            }
                            while (wait(NULL) > 0){
                            }
                            //std::cout << "BUNDLER ÇIKTI." << std::endl;
                            //std::cout << "bundler çıktı" << std::endl;
                            exit(0);
                        }
                        for(int i = 0; i < bundleCount-1; i++){
                            if(i == bundleNo || i == bundleNo-1)
                                continue;
                            //std::cout << "BU GERÇEKLEŞMEMELİ" << std::endl;
                            close(fd[i][0]);
                            close(fd[i][1]);
                        }
                        if(bundleNo != 0 && bundleNo != bundleCount-1){
                            close(fd[bundleNo-1][1]);
                            close(fd[bundleNo][0]);
                            dup2(fd[bundleNo-1][0],0);
                            dup2(fd[bundleNo][1],1);
                            close(fd[bundleNo-1][0]);
                            close(fd[bundleNo][1]);
                        } else if (bundleNo == 0) {
                            close(fd[bundleNo][0]);
                            dup2(fd[bundleNo][1],1);
                            close(fd[bundleNo][1]);
                        } else {
                            close(fd[bundleNo-1][1]);
                            dup2(fd[bundleNo-1][0],0);
                            close(fd[bundleNo-1][0]);
                        }
                    } else {
                        bundleNo = 0;
                    }
                    //std::cout << "BUNDLE NO: " << bundleNo << std::endl;
                    bundleClass currentBundle;
                    for (auto bundle: bundleVector) {
                        if (bundle.getName() == parsedInput.command.bundles[bundleNo].name) {
                            currentBundle = bundle;
                            break;
                        }
                    }
                    if(currentBundle.getOperations().size() == 0){
                        exit(0);
                    }
                    int operation_count = currentBundle.getOperations().size(), counter = -1;
                    int rep[currentBundle.getOperations().size()][2];
                    //std::cout << "BUNDLE COUNT: " << bundleCount << std::endl;
                    pid_t pid_for_forker = getpid();
                    //std::cout << "BUNDLE NO: " << bundleNo << " GİRDİ." << std::endl;
                    if(bundleNo != 0 && operation_count > 1) {
                        do{
                            counter++;
                            pipe(rep[counter]);
                            if(fork()){
                                close(rep[counter][0]);
                            } else {
                                for(int i = 0; i <= counter; i++)
                                    close(rep[i][1]);
                                dup2(rep[counter][0], 0);
                            }
                        } while (counter < operation_count-1 && pid_for_forker == getpid());
                    } else {
                        if(operation_count > 1){
                            do{
                                fork();
                                counter++;
                            } while (counter < operation_count-1 && pid_for_forker == getpid());
                        } else {
                            counter = 0;
                            fork();
                        }
                    }
                    if (pid_for_forker == getpid()) {
                        if(bundleNo != 0 && operation_count > 1) {
                            while (1) {
                                char repeater[1];
                                int repeater_count = 0;
                                //std::cout << "piç hamsi öncesi" << std::endl;
                                repeater_count = read(0, repeater, 1);
                                //std::cout << "piç hamsi" << std::endl;
                                //std::cout << "REPEATER READ:" << repeater[0] << std::endl;
                                if(repeater_count == 0){
                                    //std::cout << "piç hamsi değil" << std::endl;
                                    break;
                                }
                                for(int i = 0; i < currentBundle.getOperations().size(); i++) {
                                    //std::cout << "piçtur piç" << std::endl;
                                    //std::cout << "REPEATER WRITE:" << repeater[0] << std::endl;
                                    write(rep[i][1], repeater, repeater_count);
                                }
                            }
                            for(int i = 0; currentBundle.getOperations().size(); i++)
                                close(rep[i][1]);
                        }
                        //std::cout << "BUNDLE NO: " << bundleNo << " BEKLİYOR." << std::endl;
                        while (wait(NULL) > 0){
                            //std::cout << "BUNDLE NO: " << bundleNo << " BİRİ ÇIKTI." << std::endl;
                        }
                        // std::cout << "BUNDLE NO: " << bundleNo << " ÇIKTI." << std::endl;
                        exit(0);
                    }
                    if (parsedInput.command.bundles[bundleNo].input != NULL) {
                        input = open(parsedInput.command.bundles[bundleNo].input, O_RDONLY);
                        dup2(input, 0);
                        close(input);
                    }
                    if (parsedInput.command.bundles[bundleNo].output != NULL) {
                        output = open(parsedInput.command.bundles[bundleNo].output, O_WRONLY | O_CREAT | O_APPEND,0666);
                        dup2(output, 1);
                        close(output);
                    }
                    //std::cout << "BUNDLE NO: " << bundleNo << " OPERATION: " << counter <<" ÇIKTI." << std::endl;
                    execvp(currentBundle.getOperations().at(counter).getPathName(),&currentBundle.getOperations().at(counter).getArgv()[0]);
                }
                waitpid(master, NULL, 0);
                //std::cout << "MASTER ÇIKTI" << std::endl;
                for(int j = 0; j < parsedInput.command.bundle_count; j++, parsedInput.command.bundles++) {
                    for (int i = 0; i < bundleVector.size(); i++) {
                        if (bundleVector.at(i).getName() == parsedInput.command.bundles->name) {
                            bundleVector.erase(bundleVector.begin() + i);
                            break;
                        }
                    }
                }
            }
        }
    }
    return 0;
}
