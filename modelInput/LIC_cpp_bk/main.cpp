#include <fstream>
#include <iostream>
#include <iomanip>
#include "DT.h"
#include "find_vol_interior.h"
#include "find_vol_mod.h"
#include "readpdb.h"

int main(int argc, char *argv[])
{
  std::vector<std::map<int, int> > ResIdToType = readPDB(argv[1]);

  int num_coord, num_nsolv, num_T, num_res;
  double solvent_wgt;
  double** coord = NULL;
  double* weights = NULL;
  int* res = NULL;
  double* LIV_atom, *LIV_res, *S_atom, *LIS_atom;
  double* LV_atom, *LV_res, *LS_atom;
  double** LS_res;
  double** LIS_res;
  int check_S_res;

  check_S_res = 0;

  std::fstream dat;
  std::string headline;
  dat.open("./data/processedData", std::ios::in);
      dat >> headline;
      dat >> num_coord >> num_res;

      coord = new double*[num_coord];
      for(int i = 0; i < num_coord; ++ i) coord[i] = new double[3];
      weights = new double[num_coord];
      res = new int[num_coord];

      solvent_wgt = 0.0;//0.8;
      num_nsolv=num_coord;

      for(int i = 0; i < num_coord; ++ i) {
          dat >> coord[i][0] >> coord[i][1] >> coord[i][2] >> weights[i] >> res[i];
          weights[i] = weights[i] * weights[i] + solvent_wgt;
          if(res[i] > num_res) {
              std::cerr << "error with num_res: stop" << std::endl;
              return 0;
          } else if(res[i] < 0) {
              std::cerr << "error with res number: stop" << std::endl;
              return 0;
          } else if(res[i] == 0) {
              num_nsolv = num_nsolv - 1;
          }
      }
      dat.close();

  if (check_S_res == 1) {
      for (int i = 0; i < num_nsolv; ++ i) res[i] = i+1;
      num_res = num_nsolv;
  }

//-----------------------initialization----------------------------------------
  LIV_atom = new double[num_nsolv];
  LIV_res = new double[num_res];
  S_atom = new double[num_nsolv];
  LIS_atom = new double[num_nsolv];
  LIS_res = new double*[num_res];//allocate(LIS_res(1:num_res,1:num_res))

  for (int i = 0; i < num_res; ++ i) {
      LIS_res[i] = new double[num_res];
      LIV_res[i] = 0;
      for (int j = 0; j < num_res; ++ j)
          LIS_res[i][j] = 0;
  }

  for (int i = 0; i < num_nsolv; ++ i) {
      LIV_atom[i] = 0;
      LIS_atom[i] = 0;
      S_atom[i] = 0;
  }
//------------------------------------------------------------------------------
// there is solvent molecule in the model
/*
  if (num_nsolv != num_coord) {
      //-----------------------initialization-----------------------------------
      LV_atom = new double[num_nsolv];
      LV_res = new double[num_res];
      LS_atom = new double[num_nsolv];
      LS_res = new double*[num_res+1];//allocate(LS_res(0:num_res,0:num_res))

      for (int i = 0; i < num_nsolv; ++ i) {
          LV_atom[i] = 0;
          LS_atom[i] = 0;
      }

      for (int i = 0; i < num_res; ++ i) LV_res[i] = 0;

      for (int i = 0; i < num_res+1; ++ i) {
          LS_res[i] = new double[num_res+1];
          for (int j = 0; j < num_res+1; ++ j)
              LS_res[i][j] = 0;
      }
      //-------------------------------------------------------------------------
      find_vol_interior(num_coord, num_nsolv, coord, weights, num_res, res, LV_atom, LV_res, LS_atom, LS_res);
     // std::cerr << "num_coord " << num_coord << " num_nsolv" << num_nsolv << " num_res" << num_res << std::endl;

      std::fstream FLV_atom;
      std::fstream FLS_atom;
      FLV_atom.open("./result/LV_atom", std::ios::out);
      FLS_atom.open("./result/LS_atom", std::ios::out);
      for (int i = 0; i < num_nsolv; ++ i) {
          FLV_atom << std::setprecision(17) << std::showpoint << "   " << LV_atom[i] << "     " << std::endl;
          FLS_atom << std::setprecision(17) << std::showpoint << "   " << LS_atom[i] << "     " << std::endl;
      }
      FLV_atom.close();
      FLS_atom.close();
      std::fstream FLV_res;
      std::fstream FLS_res;
      FLV_res.open("./result/LV_res", std::ios::out);
      for (int i = 0; i < num_res; ++ i) {
          FLV_res << std::setprecision(17) << std::showpoint << "   " << LV_res[i] << "     " << std::endl;
      }
      FLV_res.close();
      FLS_res.open("./result/LS_res", std::ios::out);
      for (int i = 0; i < num_res+1; ++ i) {
          for( int j = 0; j < num_res+1; ++ j )
              FLS_res << std::setprecision(17) << std::showpoint << "   " << LS_res[i][j] << "     ";
          FLS_res << std::endl;
      }
      FLV_res.close();
      delete LV_atom;
      delete LV_res;
      delete LS_atom;
      for (int i = 0; i < num_res+1; ++ i) delete LS_res[i];
      delete LS_res;
  }
*/
  find_vol_intersection(num_nsolv, coord, weights, num_res, res,
                            LIV_atom, LIV_res, S_atom, LIS_atom, LIS_res);

  //if (check_S_res == 1) check_S_res_subroutine();
/*
  std::fstream FLIV_atom;
  std::fstream FLIS_atom;
  std::fstream FS_atom;
  FLIV_atom.open("./result/LIV_atom", std::ios::out);
  FLIS_atom.open("./result/LIS_atom", std::ios::out);
  FS_atom.open("./result/S_atom", std::ios::out);
  for (int i = 0; i < num_nsolv; ++ i) {
      FLIV_atom << std::setprecision(17) << std::showpoint << "   " << LIV_atom[i] << "     " << std::endl;
      FLIS_atom << std::setprecision(17) << std::showpoint << "   " << LIS_atom[i] << "     " << std::endl;
      FS_atom << std::setprecision(17) << std::showpoint << "   " << S_atom[i] << "     " << std::endl;
  }
  FLIV_atom.close();
  FLIS_atom.close();
  FS_atom.close();
*/
  std::fstream FLIV_res;
  std::fstream FLIS_res;
  FLIV_res.open("./result/LIV_res", std::ios::out);
  FLIS_res.open("./result/LIS_res", std::ios::out);
  for (int i = 0; i < num_res; ++ i) {
      FLIV_res << std::setprecision(17) << std::showpoint << "   " << LIV_res[i] << "     " << std::endl;
      for (int j = 0; j < num_res; ++ j)
          FLIS_res << std::setprecision(17) << std::showpoint << "   " << LIS_res[j][i] << "     ";
      FLIS_res << std::endl;
  }
  FLIV_res.close();
  FLIS_res.close();


  for(int i = 0; i < num_coord; ++ i) delete[] coord[i];


  delete[] coord;
  delete[] weights;
  delete[] res;
  coord = NULL;
  weights = NULL;
  res = NULL;
  delete LIV_atom;
  delete LIV_res;
  delete LIS_atom;
  delete S_atom;
  for (int i = 0; i < num_res; ++ i) delete LIS_res[i];
  delete LIS_res;

  return 0;
}
