/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import junit.framework.Assert;

import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import org.apache.commons.io.FileUtils;

import com.datatorrent.demos.dimensions.generic.MyInterface;

/**
 *
 */
public class GenerateClassTest
{
  private void generateClass() throws Exception
  {
    ClassNode classNode = new ClassNode(4);//4 is just the API version number
    //These properties of the classNode must be set
    classNode.version = Opcodes.V1_6;//The generated class will only run on JRE 1.6 or above
    classNode.access = Opcodes.ACC_PUBLIC;
    //classNode.signature="Lasm/Generated;";
    classNode.name = "Generated";
    classNode.superName = "java/lang/Object";
    //Create a method
    MethodNode mainMethod = new MethodNode(4, Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "main", "([Ljava/lang/String;)V", null, null);
    mainMethod.instructions.add(new FieldInsnNode(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;"));
    mainMethod.instructions.add(new LdcInsnNode("Hello World!"));
    mainMethod.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V"));
    mainMethod.instructions.add(new InsnNode(Opcodes.RETURN));
    //Add the method to the classNode
    classNode.methods.add(mainMethod);

    //Write the class
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
    classNode.accept(cw);

    // add default constructor
    MethodVisitor cv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    cv.visitVarInsn(Opcodes.ALOAD, 0);
    cv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                       "java/lang/Object", "<init>", "()V");
    cv.visitInsn(Opcodes.RETURN);
    cv.visitMaxs(1, 1);

    // add a field
    String fieldName = "stringField";
    String fieldType = "Ljava/lang/String;";
    Object initValue = null;
    cw.visitField(Opcodes.ACC_PUBLIC, fieldName, fieldType, null, initValue).visitEnd();

    //Dump the class in a file
    File outDir = new File("target/asmtest");
    outDir.mkdirs();
    DataOutputStream dout = new DataOutputStream(new FileOutputStream(new File(outDir, "Generated.class")));
    dout.write(cw.toByteArray());
    dout.flush();
    dout.close();
  }

  private void modifyClass(byte[] bytes) throws Exception
  {
    // load class
    ClassReader cr = new ClassReader(bytes);
    ClassNode classNode = new ClassNode();
    cr.accept(classNode, 0);

    // modify
    classNode.visit(4, Opcodes.ACC_PUBLIC, "Modified", null, "java/lang/Object", new String[] {"com/datatorrent/demos/dimensions/generic/MyInterface"});
    classNode.version = Opcodes.V1_6;//The generated class will only run on JRE 1.6 or above

    // Create setter
    MethodNode setterMethod = new MethodNode(4, Opcodes.ACC_PUBLIC, "setValue", "(Ljava/lang/String;)V", null, null);
    setterMethod.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    setterMethod.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    setterMethod.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, "Modified", "stringField", "Ljava/lang/String;"));

    setterMethod.instructions.add(new InsnNode(Opcodes.RETURN));
    //Add the method to the classNode
    classNode.methods.add(setterMethod);

    //Create getter method
    MethodNode methodNode = new MethodNode(4, Opcodes.ACC_PUBLIC, "getValue", "()Ljava/lang/String;", null, null);
    methodNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    methodNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, "Modified", "stringField", "Ljava/lang/String;"));
    methodNode.instructions.add(new InsnNode(Opcodes.ARETURN));//areturn
    //Add the method to the classNode
    classNode.methods.add(methodNode);

    // dump
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
    classNode.accept(cw);
    File outDir = new File("target/asmtest");
    outDir.mkdirs();
    DataOutputStream dout = new DataOutputStream(new FileOutputStream(new File(outDir, "Modified.class")));
    dout.write(cw.toByteArray());
    dout.flush();
    dout.close();
  }

  @Test
  public void test() throws Exception
  {
    String testVal = "mytestval";
    generateClass();
    byte[] bytes = FileUtils.readFileToByteArray(new File("target/asmtest/Generated.class"));
    Class<?> clazz = new ByteArrayClassLoader().defineClass("Generated", bytes);
    Method m = clazz.getMethod("main", String[].class);
    m.invoke(null, new Object[] {null});

    Object o = clazz.newInstance();

    Field f = clazz.getField("stringField");
    Assert.assertNotNull(f);

    modifyClass(bytes);

    bytes = FileUtils.readFileToByteArray(new File("target/asmtest/Modified.class"));
    clazz = new ByteArrayClassLoader().defineClass("Modified", bytes);
    Object modifyObj = clazz.newInstance();

    m = clazz.getMethod("setValue", String.class);
    m.invoke(modifyObj, testVal);
    m = clazz.getMethod("getValue");
    String result = (String)m.invoke(modifyObj);

    Assert.assertEquals("reflect getVal invoke", testVal, result);

    MyInterface obj = (MyInterface)modifyObj;
    String interfaceFieldResult = obj.getValue();

    Assert.assertEquals("interface getVal invoke", testVal, interfaceFieldResult);
  }

  private static class ByteArrayClassLoader extends ClassLoader
  {
    Class<?> defineClass(String name, byte[] ba)
    {
      return defineClass(name, ba, 0, ba.length);
    }

  }

}
