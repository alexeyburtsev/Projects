function [B,E,W]=distor3D(R,U)
%R,U-  matrices 3xN
N=max(size(R));
M=[0 0 0;0 0 0;0 0 0];
for i=1:N
    r=[R(1,i);R(2,i);R(3,i)];
           M=M+dyad(r,r);
end;
           M=inv(M);
B=[0 0 0;0 0 0;0 0 0];
for i=1:N
    r=[R(1,i);R(2,i);R(3,i)];
             g=scalMV(M,r);
    u=[U(1,i);U(2,i);U(3,i)];
             B=B+dyad(g,u);
             E=(B+B')/2;    
             W=(B-B')/2;
end;