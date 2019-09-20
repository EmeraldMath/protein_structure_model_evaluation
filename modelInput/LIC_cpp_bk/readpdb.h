#ifndef READPDB_H
#define READPDB_H
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <iomanip>
#include <map>

void rad_init(std::map<std::string, double>& Find_vdW_Radius) {
    Find_vdW_Radius["C"]=1.70;
    Find_vdW_Radius["H"]=1.09;
    Find_vdW_Radius["N"]=1.55;
    Find_vdW_Radius["O"]=1.52;
    Find_vdW_Radius["B"]=2.00;
    Find_vdW_Radius["F"]=1.47;
    Find_vdW_Radius["I"]=1.98;
    Find_vdW_Radius["K"]=2.75;
    Find_vdW_Radius["P"]=1.80;
    Find_vdW_Radius["S"]=1.80;
    Find_vdW_Radius["U"]=1.86;
}

void res_init(std::map<std::string, int>& res_table) {
    res_table["VAL"]=1;
    res_table["ILE"]=2;
    res_table["LEU"]=3;
    res_table["ALA"]=4;
    res_table["MET"]=5;
    res_table["PHE"]=6;
    res_table["HIS"]=7;
    res_table["TRP"]=8;
    res_table["TYR"]=9;
    res_table["PRO"]=10;
    res_table["THR"]=11;
    res_table["GLY"]=12;
    res_table["SER"]=13;
    res_table["GLN"]=14;
    res_table["ASN"]=15;
    res_table["ASP"]=16;
    res_table["GLU"]=17;
    res_table["ARG"]=18;
    res_table["LYS"]=19;
    res_table["CYS"]=20;
    res_table["UNK"]=21;
}

//void PDB_Read_Coordinates(std::string PDB_file, int& n_atoms, std::ofstream& outf, std::map<int, int>& idToType, std::vector<std::vector<double> >& XYZs,
void PDB_Read_Coordinates(char* PDB_file, int& n_atoms, std::ofstream& outf, std::map<int, int>& idToType, std::vector<std::vector<double> >& XYZs,
                          std::vector<int>& res, std::vector<double>& radii, int& n_res, std::ofstream& note) {
    std::vector<std::string> AtomType;
    std::string line;
    std::fstream in;
    std::map<std::string, double> Find_vdW_Radius;
    int my_index = 0;
    int n = -1;
    rad_init(Find_vdW_Radius);
    std::map<std::string, int> res_table;
    res_init(res_table);
    outf.setf(std::ios::fixed, std::ios::floatfield); 
    outf.precision(3);

    in.open(PDB_file, std::ios::in);
    if(!in.is_open()) {
    	std::cout << PDB_file << " cannot be opened!" << std::endl;
    	return;
    }
    std::string pre_index("");
    std::string cur_index;
    bool model = false;
    while(getline(in, line)) {
    	if (line.substr(0,6) == "ATOM  ") {
            model = true;
    		n ++;
            std::stringstream coord, name, residue, res_index;
            double x, y, z;
            coord << line.substr(30,24);
            coord >> x >> y >> z;
            std::vector<double> v;
            v.push_back(x);
            v.push_back(y);
            v.push_back(z);
            XYZs.push_back(v);
            name << line.substr(12,4);
            std::string atom_name;
            name >> atom_name;
            residue << line.substr(17,3);
            std::string res_name;
            residue >> res_name;
            if (res_table.find(res_name) == res_table.end()) {
                int tmp = 21;
                note << "Protein:" << PDB_file << " " << res_name << std::endl;
                res_name = "UNK";
                res_table.insert(std::make_pair(res_name,tmp));
            }
            res_index << line.substr(21,5);
            std::string chain, resNum;
            res_index >> chain >> resNum;
            cur_index = chain + resNum;
            if (cur_index != pre_index) {
                my_index ++;
                idToType.insert(std::make_pair(my_index, res_table[res_name]));
                pre_index = cur_index;
            }
            res.push_back(my_index);

            if (atom_name[0] >= '1' and atom_name[0] <= '9') {
                AtomType.push_back(atom_name.substr(1,1));
            } else {
                AtomType.push_back(atom_name.substr(0,1));
            }
            radii.push_back(Find_vdW_Radius[AtomType[n]]);
    	} else if (model && (line.substr(0,5) == "MODEL")) {
            break;
        }
    }
    n_res = my_index;
    n_atoms = n + 1;
}

//std::vector<std::map<int, int> > readPDB(std::string PDB_file) {
std::vector<std::map<int, int> > readPDB(char* PDB_file) {
    std::vector<std::map<int, int> > ResIdToType;
    //std::string dir("./data/");
    //std::ifstream files(inputF);
    //std::stringstream buffer;
    //buffer <<  files.rdbuf();
    std::ofstream outf,note;
    outf.open("./data/processedData");
    note.open("Note");
    //while (buffer >> PDB_file) {
        std::map<int, int> idToType;
        std::vector<std::vector<double> > XYZs;
        std::vector<int> res;
        std::vector<double> radii;
        int n_atoms = 0;
        int n_res = 0;
        //MARK
        //std::cout << dir+PDB_file << std::endl;
        //PDB_Read_Coordinates(dir+PDB_file, n_atoms, outf, idToType, XYZs, res, radii, n_res, note);
        PDB_Read_Coordinates(PDB_file, n_atoms, outf, idToType, XYZs, res, radii, n_res, note);
        outf << "Protein:" << std::string(PDB_file) << std::endl;
        outf << n_atoms << std::endl;
        outf << n_res << std::endl;
        for (int i = 0; i < n_atoms; ++i)
        {       
            outf << std::setw(9) <<  XYZs[i][0] << std::setw(9) << XYZs[i][1]
                << std::setw(9) << XYZs[i][2] << std::setw(8) << radii[i] << std::setw(9) << res[i] << std::endl;
        }
        ResIdToType.push_back(idToType);
    //}
    outf.close();
    note.close();
    return ResIdToType;
}

#endif
