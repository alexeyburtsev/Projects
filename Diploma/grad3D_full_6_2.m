function [g11,g12,g13,g21,g22,g23,K1,K2]=grad3D_full_6_2(f1,F,f2,L,H)
R=[1 0.5 -0.5 -1 -0.5 0.5;0 0.866 0.866 0 -0.866 -0.866];
R1=[R; 0.1 0.1 0.1 0.1 0.1 0.1];
R2=[R; -0.1 -0.1 -0.1 -0.1 -0.1 -0.1];
RR=[R1 R2];
[V1,V2,V3]=povorot(f1,F,f2,L,H);

for i=2:(H-1)
    for j=2:(L-1)
W=[V1(i,j+1) V1(i-1,j+1) V1(i-1,j) V1(i,j-1) V1(i+1,j) V1(i+1,j+1); 
   V2(i,j+1) V2(i-1,j+1) V2(i-1,j) V2(i,j-1) V2(i+1,j) V2(i+1,j+1);
   V3(i,j+1) V3(i-1,j+1) V3(i-1,j) V3(i,j-1) V3(i+1,j) V3(i+1,j+1) ]; 
WW=[W W];

g=distor3D(RR,WW); 
g11(i,j)=g(1,1);
g12(i,j)=g(1,2);
g21(i,j)=g(2,1);
g22(i,j)=g(2,2);
g13(i,j)=g(1,3);
g23(i,j)=g(2,3);  
K1(i,j)=g11(i,j)+g22(i,j);
K2(i,j)=sqrt(dscal(g));
    end;
end;