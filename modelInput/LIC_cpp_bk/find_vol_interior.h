#ifndef FIND_VOL_INTERIOR_H
#define FIND_VOL_INTERIOR_H
#include "DT.h"
#include "type.h"
#include <cmath>
//#include <algorithm>
#include <fstream>
#include <iomanip>
//#include <vector>
void find_vol(int** aedg, int& num_edg, int& num_T, int* res, int* num_ring, int** tetr_ring, int** edge_list,
              int** T, int** T_nbrs, double** coords, double* lift, double** char_pt_T, int** tetra_ring,
              double* LV_atom, double* LV_res, double* LS_atom, double** LS_res);
void compute_vol(int edg_num, double** coords, double* lift, int** edge_list, int* num_ring,
                 int** tetra_ring, double** char_pt_T, int* res, double* LV_atom, double* LV_res,
                 double* LS_atom, double** LS_res);
void test_same_plane(double x[3], int num_ring_edg, int* T_ring, double** char_pt_T);
void find_tetra_ring(int& edg_ind, int edg_num_int, int& T_ind, int** tetra_ring,
                     int** edge_list, int** T, int** T_nbrs, int* num_ring, int** aedg);
void mark_edg(int e1, int e2, int& edg_ind, int& tetra_ind, int& edg_int, int** edge_list, int** T, int** aedg);
void find_char_pt_T(double** coord, int** T, double*& lift, int& num_T, double**& char_pt_T);
void Minvb(double(&x)[4], double M[4][4], double b[4]);

void find_vol_interior(int& num_coords, int& num_nsolv, double** coords, double* weights, int& num_res,
                       int* res, double* LV_atom, double* LV_res, double* LS_atom, double** LS_res) {
    int** T;
    int** T_nbrs;
    int** aedg;
    int** edge_list;
    int** tetra_ring;
    int* upedg_T, *upedg_num, *num_ring;
    double** char_pt_T;
    double* lift = new double[num_coords];
    int num_T, num_edg;

    for(int i = 0; i < num_coords; ++ i) {
        lift[i] = 0;
        for(int j = 0; j < 3; ++ j)
            lift[i] += coords[i][j]*coords[i][j];
        lift[i] -= weights[i];
    }

    Delaunay_Tetrahedrize(num_coords, coords, weights, num_T, T, T_nbrs);

    char_pt_T = new double*[num_T];
    aedg = new int*[num_T];
    for(int i = 0; i < num_T; ++ i) {
        char_pt_T[i] = new double[3];
        aedg[i] = new int[6];
    }
    edge_list = new int*[6*num_T];
    tetra_ring = new int*[6*num_T];
    num_ring = new int[4*num_T];
    for(int i = 0; i < 6*num_T; ++ i) {
        edge_list[i] = new int[2];
        tetra_ring[i] = new int[20];
    }
    find_char_pt_T(coords, T, lift, num_T, char_pt_T);
    find_vol(aedg, num_edg, num_T, res, num_ring, tetra_ring, edge_list, T, T_nbrs, coords, lift, char_pt_T, tetra_ring,
             LV_atom, LV_res, LS_atom, LS_res);
    for(int i = 0; i < 6*num_T; ++ i) {
        delete[] edge_list[i];
        delete[] tetra_ring[i];
    }
    for(int i = 0; i < num_T; ++ i) {
        delete[] char_pt_T[i];
        delete[] aedg[i];
    }

    delete[] edge_list;
    delete[] tetra_ring;
    delete[] char_pt_T;
    delete[] aedg;
    delete[] lift;
    delete[] num_ring;
    for(int i = 0; i < num_T; ++ i){
      delete[] T[i];
      delete[] T_nbrs[i];
    }

    delete[] T;
    delete[] T_nbrs;

    T = NULL;
    T_nbrs = NULL;

}

void find_vol(int** aedg, int& num_edg, int& num_T, int* res, int* num_ring, int** tetr_ring, int** edge_list,
              int** T, int** T_nbrs, double** coords, double* lift, double** char_pt_T, int** tetra_ring,
              double* LV_atom, double* LV_res, double* LS_atom, double** LS_res) {
    for (int i = 0; i < num_T; ++ i) {
        for (int j = 0; j < 6; ++j)
            aedg[i][j] = 0;
    }
    num_edg = 0;

    for(int i = 1; i <= num_T; ++ i) {
        for(int j = 1; j <= 6; ++ j) {
            if (aedg[i-1][j-1] == 0) {
                if (j == 1) {
                    if (res[T[i-1][0]-1 ] != 0 || res[T[i-1][1]-1 ] != 0) {
                        num_edg = num_edg + 1;
                        num_ring[num_edg - 1] = 0;
                        edge_list[num_edg - 1][0] = T[i-1][0];
                        edge_list[num_edg - 1][1] = T[i-1][1];
                        aedg[i-1][0] = num_edg;
                        find_tetra_ring(num_edg, 1, i, tetr_ring, edge_list, T, T_nbrs, num_ring, aedg);
                        compute_vol(num_edg, coords, lift, edge_list, num_ring, tetra_ring, char_pt_T,
                                    res, LV_atom, LV_res, LS_atom, LS_res);
                    }
                }
                else if (j == 2) {
                    if (res[T[i-1][0]-1 ] != 0 || res[T[i-1][2]-1 ] != 0) {
                        num_edg = num_edg + 1;
                        num_ring[num_edg - 1] = 0;
                        edge_list[num_edg - 1][0] = T[i-1][0];
                        edge_list[num_edg - 1][1] = T[i-1][2];
                        aedg[i-1][1] = num_edg;
                        find_tetra_ring(num_edg, 2, i, tetr_ring, edge_list, T, T_nbrs, num_ring, aedg);
                        compute_vol(num_edg, coords, lift, edge_list, num_ring, tetra_ring, char_pt_T,
                                    res, LV_atom, LV_res, LS_atom, LS_res);
                    }
                }
                else if (j == 3) {
                    if (res[T[i-1][0]-1 ] != 0 || res[T[i-1][3]-1 ] != 0) {
                        num_edg = num_edg + 1;
                        num_ring[num_edg - 1] = 0;
                        edge_list[num_edg - 1][0] = T[i-1][0];
                        edge_list[num_edg - 1][1] = T[i-1][3];
                        aedg[i-1][2] = num_edg;
                        find_tetra_ring(num_edg, 3, i, tetr_ring, edge_list, T, T_nbrs, num_ring, aedg);
                        compute_vol(num_edg, coords, lift, edge_list, num_ring, tetra_ring, char_pt_T,
                                    res, LV_atom, LV_res, LS_atom, LS_res);
                    }
                }
                else if (j == 4) {
                    if (res[T[i-1][1]-1 ] != 0 || res[T[i-1][2]-1 ] != 0) {
                        num_edg = num_edg + 1;
                        num_ring[num_edg - 1] = 0;
                        edge_list[num_edg - 1][0] = T[i-1][1];
                        edge_list[num_edg - 1][1] = T[i-1][2];
                        aedg[i-1][3] = num_edg;
                        find_tetra_ring(num_edg, 4, i, tetr_ring, edge_list, T, T_nbrs, num_ring, aedg);
                        compute_vol(num_edg, coords, lift, edge_list, num_ring, tetra_ring, char_pt_T,
                                    res, LV_atom, LV_res, LS_atom, LS_res);
                    }
                }
                else if (j == 5) {
                    if (res[T[i-1][1]-1 ] != 0 || res[T[i-1][3]-1 ] != 0) {
                        num_edg = num_edg + 1;
                        num_ring[num_edg - 1] = 0;
                        edge_list[num_edg - 1][0] = T[i-1][1];
                        edge_list[num_edg - 1][1] = T[i-1][3];
                        aedg[i-1][4] = num_edg;
                        find_tetra_ring(num_edg, 5, i, tetr_ring, edge_list, T, T_nbrs, num_ring, aedg);
                        compute_vol(num_edg, coords, lift, edge_list, num_ring, tetra_ring, char_pt_T,
                                    res, LV_atom, LV_res, LS_atom, LS_res);
                    }
                }
                else if (j == 6) {
                    if (res[T[i-1][2]-1 ] != 0 || res[T[i-1][3]-1 ] != 0) {
                        num_edg = num_edg + 1;
                        num_ring[num_edg - 1] = 0;
                        edge_list[num_edg - 1][0] = T[i-1][2];
                        edge_list[num_edg - 1][1] = T[i-1][3];
                        aedg[i-1][5] = num_edg;
                        find_tetra_ring(num_edg, 6, i, tetr_ring, edge_list, T, T_nbrs, num_ring, aedg);
                        compute_vol(num_edg, coords, lift, edge_list, num_ring, tetra_ring, char_pt_T,
                                    res, LV_atom, LV_res, LS_atom, LS_res);
                    }
                }
            }
        }
    }
    return;
}

void compute_vol(int edg_num, double** coords, double* lift, int** edge_list, int* num_ring,
                 int** tetra_ring, double** char_pt_T, int* res, double* LV_atom, double* LV_res,
                 double* LS_atom, double** LS_res) {
    double surf = 0;
    double area_tri, t, h1, h2, inner_pro, x[3];
    int e1, e2;
    double vol;

    e1 = edge_list[edg_num-1][0];
    e2 = edge_list[edg_num-1][1];
    double p1[4] = {coords[e1-1][0], coords[e1-1][1], coords[e1-1][2], lift[e1-1]};
    double p2[4] = {coords[e2-1][0], coords[e2-1][1], coords[e2-1][2], lift[e2-1]};
    double p12[4] = {p1[0]-p2[0], p1[1]-p2[1], p1[2]-p2[2], p1[3]-p2[3]};
    t = p12[3] - 2.0*(p2[0]*p12[0]+p2[1]*p12[1]+p2[2]*p12[2]);
    t = t/2.0/(p12[0]*p12[0]+p12[1]*p12[1]+p12[2]*p12[2]);
    x[0] = p12[0]*t+p2[0];
    x[1] = p12[1]*t+p2[1];
    x[2] = p12[2]*t+p2[2];
    if (num_ring[edg_num-1] < 3) {
        std::cerr << "Error with number of tetra in ring: stop" << std::endl;
        exit(0);
    }

    int* T_ring = new int[num_ring[edg_num-1] ];
    for (int i = 0; i < num_ring[edg_num-1]; ++ i) T_ring[i] = tetra_ring[edg_num-1][i];

    test_same_plane(x, num_ring[edg_num-1], T_ring, char_pt_T);
    delete[] T_ring;
    double c1[3] = {char_pt_T[tetra_ring[edg_num-1][0]-1 ][0], char_pt_T[tetra_ring[edg_num-1][0]-1 ][1], char_pt_T[tetra_ring[edg_num-1][0]-1 ][2]};
    double c2[3] = {char_pt_T[tetra_ring[edg_num-1][1]-1 ][0], char_pt_T[tetra_ring[edg_num-1][1]-1 ][1], char_pt_T[tetra_ring[edg_num-1][1]-1 ][2]};
    double c3[3] = {char_pt_T[tetra_ring[edg_num-1][2]-1 ][0], char_pt_T[tetra_ring[edg_num-1][2]-1 ][1], char_pt_T[tetra_ring[edg_num-1][2]-1 ][2]};
    double a[3] = {c1[0]-c3[0], c1[1]-c3[1], c1[2]-c3[2]};
    double b[3] = {c1[0]-c2[0], c1[1]-c2[1], c1[2]-c2[2]};
    area_tri = 0.5*operation::abs_cross_product(a,b);
    surf += area_tri;

    for (int i = 4; i <= num_ring[edg_num-1]; ++ i) {
        for (int j = 0; j < 3; ++ j) {
            c2[j] = c3[j];
            c3[j] = char_pt_T[tetra_ring[edg_num-1][i-1]-1 ][j];
            a[j] = c1[j] - c3[j];
            b[j] = c1[j] - c2[j];
        }
        area_tri = 0.5*operation::abs_cross_product(a,b);
        surf += area_tri;
    }

    if (res[e1-1] != 0){
        h1 = sqrt(pow(x[0]-p1[0],2)+pow(x[1]-p1[1],2)+pow(x[2]-p1[2],2));
        LS_atom[e1-1] = LS_atom[e1-1] + surf;
        vol = 1.0/3.0*surf*h1;
        if (t <= 1.0) {
            LV_atom[e1-1] = LV_atom[e1-1] + vol;
            LV_res[res[e1-1]-1] = LV_res[res[e1-1]-1] + vol;
        } else {
            LV_atom[e1-1] = LV_atom[e1-1] - vol;
            LV_res[res[e1-1]-1] = LV_res[res[e1-1]-1] - vol;
        }
    }

    if (res[e2-1] != 0){
        h2 = sqrt(pow(x[0]-p2[0],2)+pow(x[1]-p2[1],2)+pow(x[2]-p2[2],2));
        LS_atom[e2-1] = LS_atom[e2-1] + surf;
        vol = 1.0/3.0*surf*h2;
        if (t >= 0) {
            LV_atom[e2-1] = LV_atom[e2-1] + vol;
            LV_res[res[e2-1]-1] = LV_res[res[e2-1]-1] + vol;
        } else {
            LV_atom[e2-1] = LV_atom[e2-1] - vol;
            LV_res[res[e2-1]-1] = LV_res[res[e2-1]-1] - vol;
        }
    }

    if (res[e1-1] != res[e2-1]) {
        LS_res[res[e1-1] ][res[e2-1] ] += surf;
        LS_res[res[e2-1] ][res[e1-1] ] += surf;
    }
    return;
}




void test_same_plane(double x[3], int num_ring_edg, int* T_ring, double** char_pt_T) {

    double p1[3], p2[3], p3[3], p_test[3], n[3], p[3], q[3];
    double test = 0;
    double eps = pow(10.0, -7);
    if (T_ring[0] == 0 || T_ring[1] == 0 || T_ring[2] == 0) {
        std::cerr << "error; edge is not interior" << std::endl;
        exit(0);
    }

    for( int i = 0; i < 3; ++ i) {
        p1[i] = char_pt_T[T_ring[0]-1 ][i];
        p2[i] = char_pt_T[T_ring[1]-1 ][i];
        p3[i] = char_pt_T[T_ring[2]-1 ][i];
        p[i] = p1[i] - p3[i];
        q[i] = p1[i] - p2[i];
    }
    operation::cross_product(p, q, n);


    for(int i = 0; i < 3; ++ i) {
        test += n[i] * (p1[i]-x[i]);
    }
    if (fabs(test) > eps) {
        std::cerr << "error with plane test: stop" << std::endl;
        std::cerr << "fabs(test), eps " <<fabs(test) << " " << eps << std::endl;
        exit(0);
    }
    for(int k = 4; k <= num_ring_edg; ++ k) {
        if (T_ring[k-1] == 0) {
            std::cerr << "error; edge is not interior" << std::endl;
            exit(0);
        }
        test = 0;
        for(int i = 0; i < 3; ++ i) {
            p_test[i] = char_pt_T[T_ring[k-1]-1 ][i];
            test += n[i] * (p1[i] - p_test[i]);
        }

        if (fabs(test) > eps) {
            std::cerr << "error with plane test: stop" << std::endl;
            exit(0);
        }
    }
    return;
}


void find_tetra_ring(int& edg_ind, int edg_num_int, int& T_ind, int** tetra_ring,
                     int** edge_list, int** T, int** T_nbrs, int* num_ring, int** aedg) {
  int current_tetra, previous_tetra, num_in_ring, num_zero;
  int current_edg_int, num_while, int_nums, tag, current_tri;

  current_edg_int = edg_num_int;
  current_tetra = T_ind;
  tetra_ring[edg_ind-1][0] = T_ind;
  num_in_ring = 1;
  num_zero = 0;
  previous_tetra = -1;
  num_while = 0;
  int e1 = edge_list[edg_ind-1][0];
  int e2 = edge_list[edg_ind-1][1];

  while (true) {
      num_while = num_while + 1;
      if (num_while > 20) {
          std::cerr << "# adjacent tetra of the edge > 20: exit" << std::endl;
          break;
      }
      tag = 0; //reset at beginning of every loop
      if (current_edg_int == 1) {
          //possible current_tris:  1,2
          if (T_nbrs[current_tetra-1][3] != previous_tetra) {
              current_tri = 1;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          } else {
              current_tri = 2;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          }
      }

      else if (current_edg_int == 2) {
          //possible current_tris:  1,3
          //find current tri
          if (T_nbrs[current_tetra-1][3] != previous_tetra) {
              current_tri = 1;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          } else {
              current_tri = 3;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          }
      }
      else if (current_edg_int == 3) {
          //possible current_tris:  2,3
          if (T_nbrs[current_tetra-1][2] != previous_tetra) {
              current_tri = 2;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          } else {
              current_tri = 3;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          }
      }
      else if (current_edg_int == 4) {
          //possible current_tris:  1,4
          if (T_nbrs[current_tetra-1][3] != previous_tetra) {
              current_tri = 1;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          } else {
              current_tri = 4;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          }
      }
      else if (current_edg_int == 5) {
          //possible current_tris:  2,4
          if (T_nbrs[current_tetra-1][2] != previous_tetra) {
              current_tri = 2;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          } else {
              current_tri = 4;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          }
      }
      else {
          //possible current_tris:  3,4
          if (T_nbrs[current_tetra-1][1] != previous_tetra) {
              current_tri = 3;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          } else {
              current_tri = 4;
              previous_tetra = current_tetra;
              current_tetra = T_nbrs[current_tetra-1][5-current_tri-1];
              if (current_tetra != 0)
                  mark_edg(e1, e2, edg_ind, current_tetra, current_edg_int, edge_list, T, aedg);
          }
      }
      if (current_tetra != T_ind) {
          num_in_ring = num_in_ring + 1;
          tetra_ring[edg_ind-1][num_in_ring-1] = current_tetra;
      } else {
          break;
      }
      if (current_tetra == 0) {
          std::cerr << "current_tetra " << current_tetra << std::endl;
          //loop around edge in other direction or exit
          if (num_zero == 0) {
              num_zero = 1;
              current_tetra = T_ind;
              previous_tetra = tetra_ring[edg_ind-1][1];
              current_edg_int = edg_num_int;
          }
          else if (num_zero == 1) break;
      }
  }
  num_ring[edg_ind-1] = num_in_ring;

  return;
}

void mark_edg(int e1, int e2, int& edg_ind, int& tetra_ind, int& edg_int, int** edge_list, int** T, int** aedg) {
    int tetra[4] = {T[tetra_ind-1][0], T[tetra_ind-1][1], T[tetra_ind-1][2], T[tetra_ind-1][3] };
    e1=edge_list[edg_ind-1][0];
    e2=edge_list[edg_ind-1][1];
//find internal edg #, mark, and return
    if (e1 == tetra[0] || e2 == tetra[0]) {
        if (e1 == tetra[1] || e2 == tetra[1])
            edg_int = 1;
        else if (e1 == tetra[2] || e2 == tetra[2])
            edg_int = 2;
        else if (e1 == tetra[3] || e2 == tetra[3])
            edg_int = 3;
    }
    else if (e1 == tetra[1] || e2 == tetra[1]) {
        if (e1 == tetra[2] || e2 == tetra[2])
            edg_int = 4;
        else if (e1 == tetra[3] || e2 == tetra[3])
            edg_int = 5;
    }
    else if (e1 == tetra[2] || e2 == tetra[2]) {
        if (e1 == tetra[3] || e2 == tetra[3]) edg_int = 6;
    }
    aedg[tetra_ind-1][edg_int-1] = edg_ind;
    return;
}

void find_char_pt_T(double** coords, int** T, double*& lift, int& num_T, double**& char_pt_T) {
    double x[4];
    std::fstream test;
    test.open("charpcpp",std::ios::out);
    for(int i = 0; i < num_T; ++ i) {
        double p1[4] = {coords[T[i][0]-1 ][0], coords[T[i][0]-1 ][1], coords[T[i][0]-1 ][2], lift[T[i][0]-1 ]};
        double p2[4] = {coords[T[i][1]-1 ][0], coords[T[i][1]-1 ][1], coords[T[i][1]-1 ][2], lift[T[i][1]-1 ]};
        double p3[4] = {coords[T[i][2]-1 ][0], coords[T[i][2]-1 ][1], coords[T[i][2]-1 ][2], lift[T[i][2]-1 ]};
        double p4[4] = {coords[T[i][3]-1 ][0], coords[T[i][3]-1 ][1], coords[T[i][3]-1 ][2], lift[T[i][3]-1 ]};
        double M[4][4] = { {2.0*p1[0], 2.0*p1[1], 2.0*p1[2], -1.0},
                           {2.0*p2[0], 2.0*p2[1], 2.0*p2[2], -1.0},
                           {2.0*p3[0], 2.0*p3[1], 2.0*p3[2], -1.0},
                           {2.0*p4[0], 2.0*p4[1], 2.0*p4[2], -1.0} };
        double b[4] = {p1[3], p2[3], p3[3], p4[3]};
        operation::Minvb(x, M, b);
        char_pt_T[i][0] = x[0];
        char_pt_T[i][1] = x[1];
        char_pt_T[i][2] = x[2];

        test << std::setprecision(17) << std::left << std::setw(25) << char_pt_T[i][0]
             << std::setprecision(17) << std::left << std::setw(25) << char_pt_T[i][1]
             << std::setprecision(17) << std::left << std::setw(25) << char_pt_T[i][2] << std::endl;

    }
    test.close();

}

#endif // FIND_VOL_INTERIOR_H
