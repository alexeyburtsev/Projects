function V=rotVE(f1,F,f2)
%f1,F,f2 - Euler angles in Bunge notation
%V - vector of rotation
e3=[0;0;1];
V1=f1*e3;
R1=RotMV(V1);
e1=R1*[1;0;0];
e2=R1*[0;1;0];
V2=F*e1;
R2=RotMV(V2);
e2=R2*e2;
e3=R2*e3;
V3=f2*e3;
R3=RotMV(V3);
R=R3*R2*R1;
V=rotVM(R);